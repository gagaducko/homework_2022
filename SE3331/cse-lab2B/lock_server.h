// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"


//
//20221105
//lab2B
//
class lock_server {

 protected:
  int nacquire;
  pthread_mutex_t mtx;
  pthread_cond_t cond;
  std::map<lock_protocol::lockid_t, bool> locks;
  std::map<lock_protocol::lockid_t, pthread_cond_t> conds;


 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 