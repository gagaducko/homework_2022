// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

//20221106

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mtx, NULL);
  pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);

  if(locks.find(lid) == locks.end()){
    pthread_cond_init(&conds[lid], NULL);
  } else if(locks[lid]) {
    bool flag = false;
    while(locks[lid]) {
      flag = true;
      pthread_cond_wait(&conds[lid], &mtx);
    }
    if(flag) {
      printf("lock_server: %d waiting for %llu\n", clt, lid);
    }
  }

  locks[lid] = true;
  r = lock_protocol::OK;
  pthread_mutex_unlock(&mtx);

  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);

  locks[lid] = false;
  pthread_cond_signal(&conds[lid]);
  r = lock_protocol::OK;
  printf("\tinfo: here s lock server release ");

  pthread_mutex_unlock(&mtx);

  return ret;
}