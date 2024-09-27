#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <random>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <set>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"
using namespace std;


////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///
/// raft.h
/// 20221126
///
////////////////////////////////////////////////////////////////////////////////////////////////////////////////



template <typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

// #define RAFT_LOG(fmt, args...) \
//     do {                       \
//     } while (0);

    #define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */

    /* ---- Volatile state on all server----  */

    /* ---- Volatile state on leader----  */
    int vote_for;
    std::vector<log_entry<command>> log;
    std::vector<char> snapshot;

    // volatile states
    int commit_index;
    int last_applied;

    // candidate volatile states
    int vote_count;
    std::vector<bool> votedNodes;

    // leader volatile states
    std::vector<int> next_index;
    std::vector<int> match_index;
    std::vector<int> match_count;

    // times
    std::chrono::system_clock::time_point lastTime;
    std::chrono::system_clock::duration fTimeout;
    std::chrono::system_clock::duration cTimeout;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);
    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);
    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);
    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);
    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void init_time();
    void recover_storage(raft_storage<command> *storage, state_machine *state);
    void volatile_states();
    inline int get_term(int index);
    std::vector<log_entry<command>> get_entries(int begin_index, int end_index);
    void to_follower(int term);
    void election_run();
    void set_into_leader();
    void heart_beat_sent();
    void update_storage(int type);
    void set_last_time();
    void commit_entries_args(int last_log_index, int i);
    void commit_snapshot_args(int i);
    void vote_self();
    void init_request_argument();
    int get_last_log_index();
    bool judge_match(int prev_log_index, int prev_log_term);
    int get_log_term(int index);
    void commit_to_applied();
    append_entries_args<command> append_entries_renew();
    bool check_args(request_vote_args args);
    void reply_success(int node, const append_entries_args<command> &arg);
    int install_snapshot_healper(install_snapshot_args args);
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) :
    stopped(false),
    rpc_server(server),
    rpc_clients(clients),
    my_id(idx),
    storage(storage),
    state(state),
    background_election(nullptr),
    background_ping(nullptr),
    background_commit(nullptr),
    background_apply(nullptr),
    current_term(0),
    role(follower) {
    //正文
    thread_pool = new ThrPool(32);
    recover_storage(storage, state);
    volatile_states();
    set_last_time();
    init_time();

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}


//new_command
template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    mtx.lock();
    if (role != leader) {
        mtx.unlock();
        return false;
    }
    term = current_term;
    index = log.back().index + 1;
    log_entry<command> entry(index, term, cmd);
    log.push_back(entry);
    next_index[my_id] = index + 1;
    match_index[my_id] = index;
    match_count.push_back(1);
    if (!storage->appendLog(entry, log.size())) {
        update_storage(1);
    }
    mtx.unlock();
    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    mtx.lock();
    snapshot = state->snapshot();
    if(last_applied > log.back().index) {
        log.clear();
    } else {
        log.erase(log.begin(), log.begin() + last_applied - log.front().index);
    }
    update_storage(0);
    mtx.unlock();
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    set_last_time();
    reply.term = current_term;
    reply.voteGranted = false;
    if(args.term >= current_term) {
        if (args.term > current_term) {
            to_follower(args.term);
        }
        if ((vote_for == -1) || (vote_for == args.candidateId)) {
            if (check_args(args)) {
                vote_for = args.candidateId;
                reply.voteGranted = true;
                storage->updateMetadata(current_term, vote_for);
            }
        }
    } else {
        mtx.unlock();
        return 0;
    }
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    if(reply.term <= current_term) {
        if (role != candidate) {
            mtx.unlock();
            return;
        }
        if (reply.voteGranted && !votedNodes[target]) {
            votedNodes[target] = true;
            vote_count++;
            if (vote_count > num_nodes() / 2) {
                set_into_leader();
            }
        }
    } else {
        to_follower(reply.term);
        mtx.unlock();
        return;
    }
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    set_last_time();
    reply.term = current_term;
    reply.success = false;
    if (arg.term < current_term) {
        mtx.unlock();
        return 0;
    }
    if (arg.term > current_term || role == candidate) {
        to_follower(arg.term);
    }
    if (arg.prevLogIndex <= log.back().index && arg.prevLogTerm == get_term(arg.prevLogIndex)) {
        if (!arg.entries.empty()) {
            if (arg.prevLogIndex < log.back().index) {
                if (arg.prevLogIndex + 1 <= log.back().index) {
                    log.erase(log.begin() + arg.prevLogIndex + 1 - log.front().index, log.end());
                }
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                update_storage(1);
            } else {
                log.insert(log.end(), arg.entries.begin(), arg.entries.end());
                if (!storage->appendLog(arg.entries, log.size())) {
                    update_storage(1);
                }
            }
        }
        if (arg.leaderCommit > commit_index) {
            commit_index = std::min(arg.leaderCommit, log.back().index);
        }
        reply.success = true;
    }
    mtx.unlock();
    return 0;
}


template <typename state_machine, typename command>
void raft<state_machine, command>::reply_success(int node, const append_entries_args<command> &arg) {
    int prev = match_index[node];
    match_index[node] = std::max(match_index[node], (int)(arg.prevLogIndex + arg.entries.size()));
    next_index[node] = match_index[node] + 1;
    int last = std::max(prev - commit_index, 0) - 1;
    for (int i = match_index[node] - commit_index - 1; i > last; i--) {
        match_count[i]++;
        if (match_count[i] > num_nodes() / 2 && get_term(commit_index + i + 1) == current_term) {
            commit_index += i + 1;
            match_count.erase(match_count.begin(), match_count.begin() + i + 1);
            break;
        }
    }
}


template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    if (reply.term > current_term) {
        to_follower(reply.term);
        mtx.unlock();
        return;
    }
    if (role != leader) {
        mtx.unlock();
        return;
    }
    if (reply.success) {
        reply_success(node, arg);
    } else {
        if(next_index[node] <= arg.prevLogIndex) {
            next_index[node] = next_index[node];
        } else {
            next_index[node] = arg.prevLogIndex;
        }
    }
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot_healper(install_snapshot_args args) {
    if (args.lastIncludedIndex <= log.back().index && args.lastIncludedTerm == get_term(args.lastIncludedIndex)) {
        int end_index = args.lastIncludedIndex;
        if (end_index <= log.back().index) {
            log.erase(log.begin(), log.begin() + end_index - log.front().index);
        } else {
            log.clear();
        }
    } else {
        log.assign(1, log_entry<command>(args.lastIncludedIndex, args.lastIncludedTerm));
    }
}


template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    set_last_time();
    reply.term = current_term;
    if (args.term < current_term) {
        mtx.unlock();
        return 0;
    }
    if (args.term > current_term || role == candidate) {
        to_follower(args.term);
    }
    install_snapshot_healper(args);
    snapshot = args.snapshot;
    state->apply_snapshot(snapshot);
    last_applied = args.lastIncludedIndex;
    if(commit_index >= args.lastIncludedIndex) {
        commit_index = commit_index;
    } else {
        commit_index = args.lastIncludedIndex;
    }
    update_storage(1);
    storage->updateSnapshot(args.snapshot);
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    mtx.lock();
    if (reply.term > current_term) {
        to_follower(reply.term);
        mtx.unlock();
        return;
    }
    if (role != leader) {
        mtx.unlock();
        return;
    }
    match_index[node] = std::max(match_index[node], arg.lastIncludedIndex);
    next_index[node] = match_index[node] + 1;
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::chrono::system_clock::time_point current_time;
    while (true) {
        if (is_stopped()) {
            return;
        }
        lock.lock();
        current_time = std::chrono::system_clock::now();
        if(role == follower) {
            if (current_time - lastTime > fTimeout) {
                election_run();
            }
        } else if(role == candidate) {
            if (current_time - lastTime > cTimeout) {
                election_run();
            }
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.
    // Only work for the leader.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    while (true) {
        if (is_stopped()) {
            return;
        }
        lock.lock();

        if (role == leader) {
            int last_log_index = this->log.back().index;
            for (int i = 0; i < num_nodes(); i++) {
                if (i != my_id) {
                    if (next_index[i] <= last_log_index) {
                        if (next_index[i] > log.front().index) {
                            commit_entries_args(last_log_index, i);
                        } else {
                            commit_snapshot_args(i);
                        }
                    }
                }
            }
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}


template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committd logs the state machine
    // Work for all the nodes.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    while (true) {
        if (is_stopped()) {
            return;
        }
        lock.lock();
        if (commit_index > last_applied) {
            commit_to_applied();
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.
    // Only work for the leader.
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    while (true) {
        if (is_stopped()) {
            return;
        }
        lock.lock();
        if (role == leader) {
            heart_beat_sent();
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Adjust param: ping period
    }
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template <typename state_machine, typename command> 
void raft<state_machine, command>::init_time() {
    static std::random_device rd;
    static std::minstd_rand gen(rd());
    static std::uniform_int_distribution<int> follower_dis(300, 500);   
    static std::uniform_int_distribution<int> candidate_dis(800, 1000);
    fTimeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(follower_dis(gen)));
    cTimeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(candidate_dis(gen)));
}

template <typename state_machine, typename command> inline int raft<state_machine, command>::get_term(int index) {
    return log[index - log.front().index].term;
}

template <typename state_machine, typename command>
inline std::vector<log_entry<command>> raft<state_machine, command>::get_entries(int begin_index, int end_index) {
    std::vector<log_entry<command>> ret;
    if (begin_index < end_index) {
        ret.assign(log.begin() + begin_index - log.front().index, log.begin() + end_index - log.front().index);
    }
    return ret;
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::to_follower(int term) {
    role = follower;
    current_term = term;
    vote_for = -1;
    storage->updateMetadata(current_term, vote_for);
    init_time();
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::vote_self() {
    vote_for = my_id;
    vote_count = 1;
    votedNodes.assign(num_nodes(), false);
    votedNodes[my_id] = true;
    storage->updateMetadata(current_term, vote_for);
    init_time();
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::init_request_argument() {
    request_vote_args args{};
    args.term = current_term;
    args.candidateId = my_id;
    args.lastLogIndex = log.back().index;
    args.lastLogTerm = log.back().term;
    //向其他的传输vote request
    for (int i = 0; i < num_nodes(); ++i) {
        if (i != my_id){
            thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
        }
    }
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::election_run() {
    role = candidate;
    ++current_term;
    vote_self();
    init_request_argument();
    set_last_time();
}


template <typename state_machine, typename command>
bool raft<state_machine, command>::check_args(request_vote_args args) {
    if(!(args.lastLogTerm > log.back().term) && !(args.lastLogTerm == log.back().term && (args.lastLogIndex >= log.back().index))) {
        return false;
    } else {
        return true;
    }
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::set_into_leader() {
    role = leader;
    // reinitialize leader volatile states
    next_index.assign(num_nodes(), log.back().index + 1);
    match_index.assign(num_nodes(), 0);
    match_index[my_id] = log.back().index;
    match_count.assign(log.back().index - commit_index, 0);
    // send initial ping
    heart_beat_sent();
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::heart_beat_sent() {
    static append_entries_args<command> args{};
    args.term = current_term;
    args.leaderId = my_id;
    args.leaderCommit = commit_index;
    for (int i = 0; i < num_nodes(); ++i) {
        if (i != my_id) {
            args.prevLogIndex = next_index[i] - 1;
            args.prevLogTerm = get_term(args.prevLogIndex);
            thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
        }
        
    }
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::set_last_time() {
    lastTime = std::chrono::system_clock::now();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::recover_storage(raft_storage<command> *storage, state_machine *state) {
    if (!storage->restore(current_term, vote_for, log, snapshot)) {
        current_term = 0;
        vote_for = -1;
        log.assign(1, log_entry<command>(0, 0));
        snapshot.clear();
        storage->updateTotal(current_term, vote_for, log, snapshot);
    }
    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::volatile_states() {
    commit_index = log.front().index;
    last_applied = log.front().index;
    vote_count = 0;
    votedNodes.assign(num_nodes(), false);
    next_index.assign(num_nodes(), 1);
    match_index.assign(num_nodes(), 0);
    match_count.clear();
}

template <typename state_machine, typename command> 
append_entries_args<command> raft<state_machine, command>::append_entries_renew() {
    append_entries_args<command> args;
    args.term = current_term;
    args.leaderId = my_id;
    args.leaderCommit = commit_index;
    return args;
}

template <typename state_machine, typename command> 
void raft<state_machine, command>::commit_entries_args(int last_log_index, int i) {
    append_entries_args<command> args;
    args.term = current_term;
    args.leaderId = my_id;
    args.leaderCommit = commit_index;
    args.prevLogIndex = next_index[i] - 1;
    args.prevLogTerm = get_term(args.prevLogIndex);
    args.entries = get_entries(next_index[i], last_log_index + 1);
    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
}


template <typename state_machine, typename command> 
void raft<state_machine, command>::commit_snapshot_args(int i) {
    install_snapshot_args args;
    args.term = current_term;
    args.leaderId = my_id;
    args.lastIncludedIndex = log.front().index;
    args.lastIncludedTerm = log.front().term;
    args.snapshot = snapshot;
    thread_pool->addObjJob(this, &raft::send_install_snapshot, i, args);
}


template<typename state_machine, typename command>
int raft<state_machine, command>::get_last_log_index() {
    return storage->log.size();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::judge_match(int prev_log_index, int prev_log_term) {
    if (get_last_log_index() < prev_log_index) {
        return false;
    }
    return get_log_term(prev_log_index) == prev_log_term;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::get_log_term(int index) {
    if (index > get_last_log_index()) {
        return -1;
    }
    if (index == 0) {
        return 0;
    }
    return storage->log[index - 1].term;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::update_storage(int type) {
    if(type == 0) {
        storage->updateSnapshot(snapshot);
        storage->updateLog(log);
    } else if(type == 1) {
        storage->updateLog(log);
    } else {
    }
    
}

template <typename state_machine, typename command>
void raft<state_machine, command>::commit_to_applied() {
    std::vector<log_entry<command>> entries;
    int begin_index = last_applied + 1;
    int end_index = commit_index + 1;
    entries = get_entries(begin_index, end_index);
    for (log_entry<command> &entry : entries) {
        state->apply_log(entry.cmd);
    }
    last_applied = commit_index;
}

#endif // raft_h