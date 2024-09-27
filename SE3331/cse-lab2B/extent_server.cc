// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

extent_server::extent_server()
{
  im = new inode_manager();
  // _persister = new chfs_persister("log"); // DO NOT change the dir name here
  // tx_id = 0;

  // // Your code here for Lab2A: recover data on startup
  // //restore logdata and checkpoint
  // printf("\tinfo: now the persister creation is ok");
  // _persister->restore_logdata();
  // printf("\tinfo: now the restore_logdata is ok!");
  // _persister->restore_checkpoint();
  // printf("\tinfo: now the restore_checkpoint is ok!");

  // //get log_entry and checkpoint_vec
  // std::vector<chfs_command> logs = _persister->get_log_entry();
  // std::vector<checkpoint_info_t> checkpoint_vec = _persister->get_checkpoint_vec();
  // // firstly, need to recover to the checkpoint snapshop
  // recover_to_snapshot(checkpoint_vec);
  // //secondly, now is time to set then all ok
  // //if logs size is 0, return
  // int size = logs.size();
  // if(size == 0) {
  //   return;
  // }
  // std::vector<chfs_command> tmp_logs;
  // //thirdly, now is to get the didnt commited logs
  // get_tx_notcommit_log(logs, tmp_logs, size);
  // //now is timr to set inodema by logs
  // set_im_bylogs(tmp_logs);

}


int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);
  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: remove %lld\n", id);
  id &= 0x7fffffff;
  im->remove_file(id);
  return extent_protocol::OK;
}

int extent_server::begin(uint64_t &tx_id){
  return extent_protocol::OK;
}

int extent_server::commit(uint64_t &tx_id){
  return extent_protocol::OK;
}




//
/// @brief lab2A
/// @param just stop
/// @param for lab2B
/// @param 20221104
//
// void extent_server::get_tx_notcommit_log(std::vector<chfs_command> logs, std::vector<chfs_command> & tx_logs, int size) {
//   int tmp_dire = 0;
//   while (tmp_dire < size){
//     if (logs[tmp_dire].type == chfs_command::CMD_BEGIN) {
//       tmp_dire++;
//       for (int i = tmp_dire; i < size; i++) {
//         if (logs[i].type == chfs_command::CMD_COMMIT) {
//           // if it is committed
//           for (int j = tmp_dire; j < i; j++) {
//             tx_logs.emplace_back(logs[j]);
//           }
//           tmp_dire++;
//         } else if(logs[i].type == chfs_command::CMD_BEGIN) {
//           //which means the tx before didnt commit yet,so need to refresh the tmp_dire;
//           tmp_dire = i;
//         } 
//       }
//     } else {
//       break;
//     }
//   }
// }

// void extent_server::set_im_bylog(chfs_command log) {
//   if(log.type == chfs_command::CMD_CREATE) {
//     im->alloc_inode(std::stoi(log.info));
//   } else if(log.type == chfs_command::CMD_PUT) {
//     im->write_file(log.inode_id, log.info.c_str(), log.info.size());
//   } else if(log.type == chfs_command::CMD_REMOVE) {
//     im->remove_file(log.inode_id);
//   }
// }

// void extent_server::set_im_bylogs(std::vector<chfs_command> logs) {
//     for(chfs_command log : logs) {
//         tx_id = log.id;
//         set_im_bylog(log);
//     }
// }

// void extent_server::recover_to_snapshot(std::vector<checkpoint_info_t> checkpoint_vec) {
//   if(!checkpoint_vec.size()) {
//     return;
//   }
//   for(auto checkpoint : checkpoint_vec){
//     if(checkpoint.type) {
//       im->alloc_inode_by_inum(checkpoint.type, checkpoint.inode_id);
//       im->write_file(checkpoint.inode_id, checkpoint.data.c_str(), checkpoint.data.size());
//     }
//   }
// }

// int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
// {
//     // alloc a new inode and return inum
//     printf("extent_server: create inode\n");
//     id = im->alloc_inode(type);

//     //create persister append log
//     char info[sizeof (uint32_t) + sizeof (extent_protocol::extentid_t)];
//     per_append_log(1, 0, std::to_string(type));

//     return extent_protocol::OK;
// }

// int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
// {
//     id &= 0x7fffffff;

//     const char * cbuf = buf.c_str();
//     int size = buf.size();

//     //put persister append log
//     per_append_log(2, id, buf);

//     im->write_file(id, cbuf, size);

//     return extent_protocol::OK;
// }

// int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
// {
//     printf("extent_server: get %lld\n", id);

//     id &= 0x7fffffff;

//     int size = 0;
//     char *cbuf = NULL;

//     im->read_file(id, &cbuf, &size);
//     if (size == 0)
//       buf = "";
//     else {
//       buf.assign(cbuf, size);
//       free(cbuf);
//     }

//     return extent_protocol::OK;
// }

// int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
// {
//     id &= 0x7fffffff;

//     extent_protocol::attr attr;
//     memset(&attr, 0, sizeof(attr));
//     im->get_attr(id, attr);
//     a = attr;

//     return extent_protocol::OK;
// }

// int extent_server::remove(extent_protocol::extentid_t id, int &)
// {
//     printf("extent_server: write %lld\n", id);
//     id &= 0x7fffffff;

//     //remove append log
//     per_append_log(3, id, "");

//     im->remove_file(id);

//     return extent_protocol::OK;
// }

// void extent_server::per_append_log(int type, extent_protocol::extentid_t id, std::string buf) {
//   switch (type)
//   {
//     case 1:
//     //case 1: create
//       _persister->append_log(*(new chfs_command(chfs_command::CMD_CREATE, tx_id, id, buf)));
//       break;
//     case 2:
//     //case 2: put
//       _persister->append_log(*(new chfs_command(chfs_command::CMD_PUT, tx_id, id, buf)));
//       break;
//     case 3:
//     //case 3: remove
//       _persister->append_log(*(new chfs_command(chfs_command::CMD_CREATE, tx_id, id, buf)));
//       break;
//     case 4:
//     //case 4: begin
//       _persister->append_log(*(new chfs_command(chfs_command::CMD_BEGIN, tx_id, id, buf)));
//       break;
//     case 5:
//     //case 5: COMMIT
//       _persister->append_log(*(new chfs_command(chfs_command::CMD_COMMIT, tx_id, id, buf)));
//       break;
//     default:
//       break;
//   }
// }

// int extent_server::begin(uint64_t &tx_id) {
//     printf("extent_server: begin");
//     tx_id += 1;
//     tx_id = this->tx_id;
//     //append begin
//     per_append_log(4, 0, "");
//     return extent_protocol::OK;
// }

// int extent_server::commit(uint64_t &tx_id) {
//     printf("extent_server: commit");
//     per_append_log(5, 0, "");
//     if(_persister->is_need_check_point(_persister->file_path_logfile)){
//         std::string buf = get_files_all();
//         _persister->checkpoint(buf);
//     }
//     return extent_protocol::OK;
// }


// std::string num_to_str(int num) {
//     std::string str = std::to_string(num);
//     while(str.size() < 16) {
//       str = "0" + str;
//     }
//     return str;
// }


// void extent_server::buf_append(std::string & buf, int type, int size, int inode_id, std::string data) {
//   std::string str_type = num_to_str(type);
//   std::string str_size = num_to_str(size);
//   std::string str_inode_id = num_to_str(inode_id);
//   buf.append(str_type);
//   buf.append(str_size);
//   buf.append(str_inode_id);
//   buf.append(data);
// }


// std::string extent_server::get_files_all() {
//     printf("\t\tnow is get the all files and order to do the checkpoint");
//     std::string buf = "";
//     extent_protocol::attr attr;
//     std::string data = "";
//     //get all the fs s inode s file and type
//     for (int i = 1; i < INODE_NUM; i++) {
//         this->get(i, data);
//         this->getattr(i, attr);
//         //if inode is not alloc, just remenber a type is ok
//         if(attr.type == 0){
//             buf.append(num_to_str(attr.type));
//         } else {
//           uint32_t size = data.size();
//           buf_append(buf, attr.type, size, i, data);
//         }
//     }
//     return buf;
// }
