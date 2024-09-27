// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;
  uint64_t tx_id;
 public:
  extent_server();

  int create(uint32_t type, extent_protocol::extentid_t &id);
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
  // Your code here for lab2A: add logging APIs
  int commit(uint64_t & tx_id);
  int begin(uint64_t & tx_id);

  //some helper functions
  std::string get_files_all();
  void recover_to_snapshot(std::vector<checkpoint_info_t> checkpoint_vec);
  void buf_append(std::string & buf, int type, int size, int inode_id, std::string data);
  void per_append_log(int type, extent_protocol::extentid_t id, std::string buf);
  void set_im_bylogs(std::vector<chfs_command> logs);
  void get_tx_notcommit_log(std::vector<chfs_command> logs, std::vector<chfs_command> & tx_logs, int size);
  void set_im_bylog(chfs_command log);
};

#endif 







