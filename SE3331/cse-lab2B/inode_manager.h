// inode layer interface.

#ifndef inode_h
#define inode_h

#include <stdint.h>
#include "extent_protocol.h"
#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <iostream>
#include <string>


//
//20221026
//lab2A


#define DISK_SIZE  1024*1024*16
#define BLOCK_SIZE 512
#define BLOCK_NUM  (DISK_SIZE/BLOCK_SIZE)
#define MIN(a, b) ((a)<(b) ? (a) : (b))
#define MAX(a, b) ((a)>(b) ? (a) : (b))

typedef uint32_t blockid_t;

// disk layer -----------------------------------------

class disk {
 private:
  unsigned char blocks[BLOCK_NUM][BLOCK_SIZE];

 public:
  disk();
  void read_block(uint32_t id, char *buf);
  void write_block(uint32_t id, const char *buf);
};

// block layer -----------------------------------------

typedef struct superblock {
  uint32_t size;
  uint32_t nblocks;
  uint32_t ninodes;
} superblock_t;

class block_manager {
 private:
  disk *d;
  std::map <uint32_t, int> using_blocks;
 public:
  block_manager();
  struct superblock sb;

  uint32_t alloc_block();
  void free_block(uint32_t id);
  void read_block(uint32_t id, char *buf);
  void write_block(uint32_t id, const char *buf);
  bool is_free_block(blockid_t blockid);
};

// inode layer -----------------------------------------

#define INODE_NUM  1024

// Inodes per block.
#define IPB           1
//(BLOCK_SIZE / sizeof(struct inode))

// Block containing inode i
#define IBLOCK(i, nblocks)     ((nblocks)/BPB + (i)/IPB + 3)

// Bitmap bits per block
#define BPB           (BLOCK_SIZE*8)

// Block containing bit for block b
#define BBLOCK(b) ((b)/BPB + 2)

#define NDIRECT 100
#define NINDIRECT (BLOCK_SIZE / sizeof(uint))
#define MAXFILE (NDIRECT + NINDIRECT)


#define DATABLOCK IBLOCK(INODE_NUM, sb.nblocks) + 1

typedef struct inode {
  short type;
  unsigned int size;
  unsigned int atime;
  unsigned int mtime;
  unsigned int ctime;
  blockid_t blocks[NDIRECT+1];
  inode() {};
  inode(short type, uint32_t atime, uint32_t mtime, uint32_t ctime) : type(type), atime(atime), mtime(mtime), ctime(ctime) {}
} inode_t;

class inode_manager {
  private:
    block_manager *bm;
    struct inode *get_inode(uint32_t inum);
    void put_inode(uint32_t inum, struct inode *ino);
    uint32_t last_fit_inum = 1;

  public:
    inode_manager();
    uint32_t alloc_inode(uint32_t type);
    void free_inode(uint32_t inum);
    void read_file(uint32_t inum, char **buf, int *size);
    void write_file(uint32_t inum, const char *buf, int size);
    void remove_file(uint32_t inum);
    void get_attr(uint32_t inum, extent_protocol::attr &a);
    blockid_t get_blockid_helper(inode_t *ino, uint32_t n);
    blockid_t get_blockid(inode_t *ino, uint32_t n);
    void api_read_get_block_helper(inode * ino, int i, char * buf_out, int size);
    void bm_wrt_helper(inode * inode, int i, const char * buf);
    uint32_t iblock_helper(uint32_t inum);
    void alloc_block_by_index(inode_t *inode, uint32_t index);
    void free_block_by_index(inode_t *inode, uint32_t index);
    void alloc_inode_by_inum(uint32_t type, uint32_t inum);
    blockid_t get_blockid_by_index(inode_t * inode, uint32_t index);
};

#endif

