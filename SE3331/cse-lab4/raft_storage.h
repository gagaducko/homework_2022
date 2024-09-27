#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

//////////////////////////////////////////////////////////////////////////////
//
//  20221124
//  raft_storage.h
//
//////////////////////////////////////////////////////////////////////////////

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void init(const std::string &dir);
    void destory();
    //update
    bool updateMetadata(int term, int vote);
    bool updateSnapshot(const std::vector<char> &snapshot);
    bool updateLog(const std::vector<log_entry<command>> &log);
    bool updateTotal(int term, int vote, const std::vector<log_entry<command>> &log, const std::vector<char> &snapshot);
    //append
    bool appendLog(const log_entry<command> &log, int new_size);
    bool appendLog(const std::vector<log_entry<command>> &log, int new_size);
    //restore
    bool restore_log(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot);
    bool restore_metadata(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot);
    bool restore_snapshot(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot);
    bool restore(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot);

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string dir_metadata;
    std::string dir_log;
    std::string dir_snapshot;
    char *buf;
    int buf_size;
};

template <typename command> 
void raft_storage<command>::init(const std::string &dir) {
    buf_size = 16;
    buf = new char[buf_size];
    dir_log = dir + "/log";
    dir_metadata = dir + "/metadata";
    dir_snapshot = dir + "/snapshot";
}

template <typename command> 
void raft_storage<command>::destory() {
    delete[] buf;
}

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    init(dir);
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
    destory();
}


template <typename command> bool 
raft_storage<command>::appendLog(const log_entry<command> &entry, int new_size) {
    mtx.lock();
    std::fstream fs(dir_log, std::ios::out | std::ios::in | std::ios::binary);
    if (fs.fail()) {
        mtx.unlock();
        return false;
    }
    int size = 0;
    fs.seekp(0, std::ios::end);
    fs.write((const char *)&entry.index, sizeof(int));
    fs.write((const char *)&entry.term, sizeof(int));
    size = entry.cmd.size();
    fs.write((const char *)&size, sizeof(int));
    if (size > buf_size) {
        destory();
        buf_size = std::max(size, 2 * buf_size);
        buf = new char[buf_size];
    }
    entry.cmd.serialize(buf, size);
    fs.write(buf, size);
    fs.seekp(0, std::ios::beg);
    fs.write((const char *)&new_size, sizeof(int));
    fs.close();
    mtx.unlock();
    return true;
}

template <typename command>
bool raft_storage<command>::appendLog(const std::vector<log_entry<command>> &log, int new_size) {
    mtx.lock();
    std::fstream fs(dir_log, std::ios::out | std::ios::in | std::ios::binary);
    if (fs.fail()) {
        mtx.unlock();
        return false;
    }

    int size = 0;
    fs.seekp(0, std::ios::end);
    for (const log_entry<command> &entry : log) {
        fs.write((const char *)&entry.index, sizeof(int));
        fs.write((const char *)&entry.term, sizeof(int));
        size = entry.cmd.size();
        fs.write((const char *)&size, sizeof(int));
        if (size > buf_size) {
            delete[] buf;
            buf_size = std::max(size, 2 * buf_size);
            buf = new char[buf_size];
        }
        entry.cmd.serialize(buf, size);
        fs.write(buf, size);
    }
    fs.seekp(0, std::ios::beg);
    fs.write((const char *)&new_size, sizeof(int));
    fs.close();
    mtx.unlock();
    return true;
}


template <typename command> 
bool raft_storage<command>::updateMetadata(int term, int vote) {
    mtx.lock();
    std::fstream fs(dir_metadata, std::ios::out | std::ios::trunc | std::ios::binary);
    if (fs.fail()) {
        mtx.unlock();
        return false;
    }
    fs.write((const char *)&term, sizeof(int));
    fs.write((const char *)&vote, sizeof(int));
    fs.close();
    mtx.unlock();
    return true;
}

template <typename command> 
bool raft_storage<command>::updateLog(const std::vector<log_entry<command>> &log) {
    mtx.lock();
    std::fstream fs(dir_log, std::ios::out | std::ios::trunc | std::ios::binary);
    if (fs.fail()) {
        mtx.unlock();
        return false;
    }
    int size = log.size();
    fs.write((const char *)&size, sizeof(int));
    for (const log_entry<command> &entry : log) {
        fs.write((const char *)&entry.index, sizeof(int));
        fs.write((const char *)&entry.term, sizeof(int));
        size = entry.cmd.size();
        fs.write((const char *)&size, sizeof(int));
        if (size > buf_size) {
            destory();
            buf_size = std::max(size, 2 * buf_size);
            buf = new char[buf_size];
        }
        entry.cmd.serialize(buf, size);
        fs.write(buf, size);
    }
    fs.close();
    mtx.unlock();
    return true;
}

template <typename command> 
bool raft_storage<command>::updateSnapshot(const std::vector<char> &snapshot) {
    mtx.lock();
    std::fstream fs(dir_snapshot, std::ios::out | std::ios::trunc | std::ios::binary);
    if (fs.fail()) {
        mtx.unlock();
        return false;
    }
    int size = snapshot.size();
    fs.write((const char *)&size, sizeof(int));
    fs.write(snapshot.data(), size);
    fs.close();
    mtx.unlock();
    return true;
}

template <typename command>
bool raft_storage<command>::updateTotal(int term, int vote, const std::vector<log_entry<command>> &log, const std::vector<char> &snapshot) {
    if(updateMetadata(term, vote) && updateLog(log) && updateSnapshot(snapshot)) {
        return true;
    } else {
        return false;
    }
}

template <typename command>
bool raft_storage<command>::restore_log(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot) {
    mtx.lock();
    std::fstream fs;
    fs.open(dir_log, std::ios::in | std::ios::binary);
    if (fs.fail() || fs.eof()) { 
        //当是空的或者没有file
        mtx.unlock();
        return false;
    }
    int size = 0;
    fs.read((char *)&size, sizeof(int));
    log.resize(size);
    for (log_entry<command> &entry : log) {
        fs.read((char *)&entry.index, sizeof(int));
        fs.read((char *)&entry.term, sizeof(int));
        fs.read((char *)&size, sizeof(int));
        if (size > buf_size) {
            destory();
            buf_size = std::max(size, 2 * buf_size);
            buf = new char[buf_size];
        }
        fs.read(buf, size);
        entry.cmd.deserialize(buf, size);
    }
    fs.close();
    mtx.unlock();
}

template <typename command>
bool raft_storage<command>::restore_metadata(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot) {
    mtx.lock();
    std::fstream fs;
    fs.open(dir_metadata, std::ios::in | std::ios::binary);
    if (fs.fail() || fs.eof()) { 
        //当是空的或者没有file
        mtx.unlock();
        return false;
    }
    fs.read((char *)&term, sizeof(int));
    fs.read((char *)&vote, sizeof(int));
    fs.close();
    mtx.unlock();
}

template <typename command>
bool raft_storage<command>::restore_snapshot(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot) {
    mtx.lock();
    std::fstream fs;
    fs.open(dir_snapshot, std::ios::in | std::ios::binary);
    if (fs.fail() || fs.eof()) {
        //当是空的或者没有file
        mtx.unlock();
        return false;
    }
    int size = 0;
    fs.read((char *)&size, sizeof(int));
    snapshot.resize(size);
    for (char &c : snapshot) {
        fs.read(&c, sizeof(char));
    }
    fs.close();
    mtx.unlock();
    return true;
}

template <typename command>
bool raft_storage<command>::restore(int &term, int &vote, std::vector<log_entry<command>> &log, std::vector<char> &snapshot) {
    if(restore_metadata(term, vote, log, snapshot) && restore_log(term, vote, log, snapshot) && restore_snapshot(term, vote, log, snapshot)) {
        return true;
    } else {
        return false;
    }
}

#endif // raft_storage_h