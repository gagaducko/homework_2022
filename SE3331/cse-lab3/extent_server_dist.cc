#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const {
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0) {
        return this->raft_group->nodes[0];
    } else {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {
    // Lab3: your code here
    int leader = this->raft_group->check_exact_one_leader();
    chfs_command_raft cmd(chfs_command_raft::CMD_CRT, type, 0, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if(!(cmd.res)->done) {
        ASSERT((cmd.res)->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout, "create command wrong");
    }
    id = (cmd.res)->id;
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &) {
    // Lab3: your code here
    int leader = this->raft_group->check_exact_one_leader();
    chfs_command_raft cmd(chfs_command_raft::CMD_PUT, type, 0, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if(!(cmd.res)->done) {
        ASSERT((cmd.res)->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2000)) == std::cv_status::no_timeout, "put command wrong");
    }
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {
    // Lab3: your code here
    int leader = this->raft_group->check_exact_one_leader();
    chfs_command_raft cmd(chfs_command_raft::CMD_GET, 0, id, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!(cmd.res)->done) {
        ASSERT((cmd.res)->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout, "get command wrong");
    }
    buf = (cmd.res)->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    // Lab3: your code here
    int leader = this->raft_group->check_exact_one_leader();
    chfs_command_raft cmd(chfs_command_raft::CMD_GETA, 0, id, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!(cmd.res)->done) {
        ASSERT((cmd.res)->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout, "getattr command wrong");
    }
    a = (cmd.res)->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &) {
    // Lab3: your code here
    int leader = this->raft_group->check_exact_one_leader();
    chfs_command_raft cmd(chfs_command_raft::CMD_RMV, 0, id, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    if (!(cmd.res)->done) {
        ASSERT((cmd.res)->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(2500)) == std::cv_status::no_timeout, "remove command wrong");
    }
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist() {
    delete this->raft_group;
}