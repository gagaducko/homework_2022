#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <algorithm>
#include <vector>
#include "rpc.h"

#define MAX_LOG_SZ 131072

using namespace std;


//
//20221023
//cse-lab2A
/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */

typedef struct checkpoint_info
{
    /* data */
    int inode_id;
    int type;
    std::string data;
}checkpoint_info_t;

class chfs_command {
public:
    typedef unsigned long long txid_t;
    enum cmd_type {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        
        // need to add command types such as create
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE
    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;

    //add a inode_id
    uint64_t inode_id = 0;

    //add a info
    std::string info = "";

    // constructor
    chfs_command(): type(CMD_BEGIN),id(0),inode_id(0),info("") {};
    chfs_command(cmd_type type): type(type){};
    //with type id inodeid info
    chfs_command(cmd_type type, txid_t id, uint64_t inode_id, const std::string& info): type(type), id(id), inode_id(inode_id), info(info){};
    //with cmd
    chfs_command(const chfs_command &cmd): type(cmd.type), id(cmd.id), inode_id(cmd.inode_id), info(cmd.info){} ;

    std::string num_to_string(int num) {
        std::string str = std::to_string(num);
        while(str.size() < 20)
        {
            str = "0" + str;
        }
        return str;
    };

    //memcpy内存拷贝
    //void *memcpy(void*dest, const void *src, size_t n);
    //由src指向地址为起始地址的连续n个字节的数据复制到以destin指向地址为起始地址的空间内。
    void cmd_to_str(char *buf) {
        uint32_t info_size = info.size();
        memcpy(buf, &type, sizeof(cmd_type));
        memcpy(buf + sizeof(cmd_type), &id, sizeof(txid_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t), &inode_id, sizeof(uint64_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t), &info_size, sizeof(uint32_t));
        memcpy(buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + sizeof(uint32_t) , info.c_str(), info_size);
    }

    void str_to_cmd(const char *buf) {
        uint32_t info_size;
        memcpy(&type, buf, sizeof(cmd_type));
        memcpy(&id, buf + sizeof(cmd_type), sizeof(txid_t));
        memcpy(&inode_id, buf + sizeof(cmd_type) + sizeof(txid_t), sizeof(uint64_t));
        memcpy(&info_size, buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t), sizeof(uint32_t));
        info.resize(info_size);
        memcpy(&info[0], buf + sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t) + sizeof(uint32_t), info_size);
    }

    uint64_t size() const {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + sizeof(uint64_t)+ sizeof(uint32_t) + info.size();
        return s;
    }

};


/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(command& log);
    void checkpoint(std::string & buf);

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();


    //more helpful functions
    char* get_log_str(command &log);
    void snapshot_checkpoint(std::string &buf);
    bool is_need_check_point(std::string filename);
    void del_log_committed(std::vector<chfs_command> logs);
    void commit_vec_txid_erase(std::vector<chfs_command> & logs);
    void checkpoint_vec_adder(int inode_id, int type, std::string data);
    int api_read_file_helper(std::ifstream & in);

    //some return functions
    std::string get_file_path_logfile() {
        return file_path_logfile;
    }

    std::vector<command> get_log_entry(){
        return log_entries;
    }

    std::vector<checkpoint_info_t> get_checkpoint_vec() {
        return checkpoint_vec;
    }

    std::string get_file_path_chenkpoint() {
        return file_path_checkpoint;
    }
    

    std::string file_path_logfile;

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    
    // restored log data
    std::vector<command> log_entries;
    std::vector<checkpoint_info_t> checkpoint_vec;
};



template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    printf("now is in persister!");
    printf("the command is: ");
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A
    printf("\t now is ~persister~\n");
}

template<typename command>
char * persister<command>::get_log_str(command &log) {
    uint32_t size_log = log.size();
    char * buf = new char[size_log];
    log.cmd_to_str(buf);
    return buf;
}

template<typename command>
void persister<command>::append_log(command& log) {
    // Your code here for lab2A
    uint32_t log_size = log.size();
    char *buf = new char [log_size];
    // make the log to string
    log.cmd_to_str(buf);
    std::ofstream out(file_path_logfile, std::ios::binary | std::ios::app);
    out.write((char*)&(log_size), sizeof(uint32_t));
    out.write(buf, log_size);
    out.close();
}


template<typename command>
bool persister<command>::is_need_check_point(std::string filename) {
    const char * fileName = filename.c_str();
    if (fileName == NULL) {
        return 0;
    } else {
        //_stat函数用来获取指定路径的文件或者文件夹的信息。
        // learn from https://www.cnblogs.com/yaowen/p/4801541.html
        //thirdly,get the file size
        struct stat statbuf;
        stat(fileName, &statbuf);
        size_t filesize = statbuf.st_size;
        return filesize > MAX_LOG_SZ * 0.8;
    }
}


template<typename command>
void persister<command>::checkpoint(std::string &buf) {
    // Your code here for lab2A
    //firstly, need to restore_logdata() and so get the need info
    restore_logdata();
    //secondly, get the log entry;
    std::vector<chfs_command> logs = get_log_entry();
    //and erase the log entry that committed
    commit_vec_txid_erase(logs);
    //thirdly,  del the log that has committed
    del_log_committed(logs);
    //forthly, add snapshot to the file checkshot
    snapshot_checkpoint(buf);
}

template<typename command>
void persister<command>::commit_vec_txid_erase(std::vector<chfs_command> & logs) {
    std::vector<chfs_command::txid_t> commit_vector_tx;
    for (std::vector<chfs_command>::iterator it = logs.begin();  it != logs.end() ; it++) {
        if((*it).type == chfs_command::CMD_COMMIT){
            commit_vector_tx.template emplace_back((*it).id);
        }
    }
    for (std::vector<chfs_command>::iterator it = logs.begin();  it != logs.end() ; ) {
        if(std::count(commit_vector_tx.begin(), commit_vector_tx.end(), (*it).id)){
            it = logs.erase(it);
        } else {
            it++;
        }
    }
}

template<typename command>
void persister<command>::del_log_committed(std::vector<chfs_command> logs) {
    std::ofstream out(file_path_logfile, std::ios::trunc | std::ios::binary);
    for (std::vector<chfs_command>::iterator it = logs.begin();  it != logs.end() ; it++) {
        uint32_t log_size = (*it).size();
        char *buf_log = new char [log_size];
        (*it).cmd_to_str(buf_log);
        out.write((char*)&(log_size), sizeof(uint32_t));
        out.write(buf_log, log_size);
    }
    out.close();
}

template<typename command>
void persister<command>::snapshot_checkpoint(std::string &buf) {
    //doing the snapshot
    const char * snapshot = buf.c_str();
    std::ofstream checkfile(file_path_checkpoint, std::ios::binary);
    if(!checkfile.is_open()) {
        printf("\tcheckpoint file cannot open\n");
    } else {
        checkfile.write(snapshot, buf.size());
        checkfile.close();
    }   
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    printf("\tinfo: now is in restore_logdata\n");
    std::ifstream in(file_path_logfile , std::ios::binary);
    if(in.is_open()){
        uint32_t log_size;
        log_entries.clear();
        while (in.peek() != EOF) {
            command cmd;
            in.read((char*)&(log_size), sizeof(uint32_t));
            char buf[log_size];
            in.read(buf, log_size);
            cmd.str_to_cmd(buf);
            log_entries.template emplace_back(cmd);
        }
    } else {
        printf("\tinfo: restore_logdata but open failed sadly\n");
        return ;
    }
}

template<typename command>
int persister<command>::api_read_file_helper(std::ifstream & in) {
    char buf[24];
    in.read(buf, 16);
    return std::stoi(std::string(buf, buf + 16));
}

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A
    //read files and set into checkpoint_vec
    std::ifstream in(file_path_checkpoint, std::ios::binary);
    if(in.is_open()) {
        std::string data="";
        uint32_t inode_id, type, size;
        while (in.peek() != EOF){
            // read file type
            type = api_read_file_helper(in);
            printf("now have read the type is %d\n", type);
            if(type) {
                // read file type
                size = api_read_file_helper(in);
                printf("now have read the size is %d\n", size);
                // read file inode_id
                inode_id = api_read_file_helper(in);
                printf("now have read the inode_id is %d\n", inode_id);
                //get the data
                char tmp_data[size];
                in.read(tmp_data, size);
                data = (std::string(tmp_data, tmp_data + size));
                checkpoint_vec_adder(inode_id, type, data);
            } else {
                checkpoint_vec_adder(0, 0, "");
            }
        }
    } else {
        printf("\trestore_chekpoint failed as the file can not open!");
    }
    in.close();
}


template<typename command>
void persister<command>::checkpoint_vec_adder(int inode_id, int type, std::string data) {
    checkpoint_info_t stu;
    stu.inode_id = inode_id;
    stu.type = type;
    stu.data = data;
    checkpoint_vec.template emplace_back(stu);
}

using chfs_persister = persister<chfs_command>;

#endif // persister_h