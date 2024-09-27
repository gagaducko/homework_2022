#include "inode_manager.h"

//
//20221003
// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

//读写硬盘
void
disk::read_block(blockid_t id, char *buf)
{
  //排除错误情况
  if(!(id < 0 || buf == NULL || id >= BLOCK_NUM)) {
    //part1A
    //memcpy内存拷贝
    //void *memcpy(void*dest, const void *src, size_t n);
    //由src指向地址为起始地址的连续n个字节的数据复制到以destin指向地址为起始地址的空间内。
    printf("\tinfo: now is read_block\n");
    memcpy(buf, blocks[id], BLOCK_SIZE);
  }
  return;
}

void
disk::write_block(blockid_t id, const char *buf)
{
  //part1A
  //类read
  if(!(id < 0 || buf == NULL || id >= BLOCK_NUM)) {
    //part1A
    //memcpy内存拷贝
    //void *memcpy(void*dest, const void *src, size_t n);
    //由src指向地址为起始地址的连续n个字节的数据复制到以destin指向地址为起始地址的空间内。
    printf("\tinfo: now is write_block\n");
    memcpy(blocks[id], buf, BLOCK_SIZE);
  }
  return;
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  //file block
  int file_block = IBLOCK(INODE_NUM, sb.nblocks) + 1;
  int n = file_block;
  //分配成功的时候
  while(n < BLOCK_NUM) {
    if(!using_blocks[n]) {
      using_blocks[n] = true;
      return n;
    }
    n++;
  }
  //分配失败了
  printf("\tinfo: error, alloc_block failed!\n");
  return 0;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  //free_block只需要直接给using_blocks[]赋值为false便能将之置于未分配状态
  //置false释放block
  using_blocks[id] = false;
  return;
}


// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    // printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

void update_it(inode * ino, uint32_t type, int size, int daty) {
  ino->atime = (unsigned int) time (NULL);
  ino->mtime = (unsigned int) time (NULL);
  ino->ctime = (unsigned int) time (NULL);
  if(daty == 0) {
    ino->type = type;
  }
	if(daty == 1) {
    ino->size = size;
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  //part1A
  //用于分配inode
	int num_inode = 0;
	for(int i = 0; i < INODE_NUM; i++) {
		num_inode++;
    num_inode = num_inode % INODE_NUM;
		inode_t * inode = get_inode(num_inode);
		if(inode) {
			//如果能查询到z就是说这个num_inode是不能用的
			//释放inode，防止内存泄漏
			free(inode);
      continue;
		} else {
			//否则是可以用的
			inode = (inode_t*) malloc (sizeof(inode_t));
		  //将inode区域置为0
		  bzero(inode, sizeof(inode_t));
      update_it(inode, type, 1000, 0);
			//将inode写入磁盘
			put_inode(num_inode, inode);
		  //释放inode，防止内存泄漏
			free(inode);
			break;	
		}
	}
  //the normal inode block should begin from the 2nd inode block
  //the 1st is userd for root_dir
  if(num_inode == 0) {
    return 0;
  } else {
    return num_inode;
  }
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */

  // printf("\tinfo: now is IN free inode %d\n", inum);
  inode_t *inode = get_inode(inum);
  if(inode) {
    //要检查 inode 的状态，防止二次释放空间
    if(inode->type == 0) {
      return;
    } else {
      //free_inode
      inode->type = 0;
      put_inode(inum, inode);
      free(inode);
    }
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino;
  /* 
   * your code goes here.
   */
  //当inum out of range的时候
  if (inum < 0 || inum >= INODE_NUM) {
    /* code */
    // printf("\tinfo: inum is out of range\n");
    return NULL;
  }
  uint32_t bid = iblock_helper(inum);
  char buf[BLOCK_SIZE];
  bm->read_block(bid, buf);
  struct inode * ino_disk;
  ino_disk = (struct inode*) buf + inum % IPB;
  //当inode不存在时
  ino = (struct inode*) malloc (sizeof(struct inode));
  if(ino_disk->type == 0) {
    /* code */
    return NULL;
  } else {
    /* code */
  }
  *ino = *ino_disk;
  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  // printf("\tim: put_inode %d\n", inum);
  if (ino == NULL) {
    return;
  }
  
  bm->read_block(iblock_helper(inum), buf);
  ino_disk = (struct inode*)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(iblock_helper(inum), buf);
}

uint32_t inode_manager::iblock_helper(uint32_t inum) {
  return IBLOCK(inum, bm->sb.nblocks);
}

void inode_manager::api_read_get_block_helper(inode * ino, int i, char * buf_out, int size) {
  char buf[BLOCK_SIZE];
  bm->read_block(get_blockid_helper(ino, i), buf);
  memcpy(buf_out + i*BLOCK_SIZE, buf, size);
}

blockid_t inode_manager::get_blockid(inode_t *ino, uint32_t n) {
  //获得blockid
  char buf[BLOCK_SIZE];
  blockid_t ret;
  if (n < NDIRECT) {
    ret = ino->blocks[n];
  } else if (n < MAXFILE) {
    if (!ino->blocks[NDIRECT]) {
      printf("\tinfo: get_blockid_helper none NINDIRECT!\n");
    }
    bm->read_block(ino->blocks[NDIRECT], buf);      
    ret = ((blockid_t *)buf)[n - NDIRECT];
  } else{
    printf("\tinfo: get_nth_blockid out of range\n");
    exit(0);
  }
  return ret;
}

blockid_t inode_manager::get_blockid_helper(inode_t *ino, uint32_t n)
{
  //做边界情况的排除
  if(ino == NULL) {
    printf("\tinfo: ino is NULL\n");
    return 0;
  } else {
    /* code */
  }
  return get_blockid(ino, n);
}

void
inode_manager::alloc_block_by_index(inode_t *inode, uint32_t index) {
    blockid_t blockid = this->bm->alloc_block();

    if (blockid != -1) {
        if (index < NDIRECT) {
            inode->blocks[index] = blockid;
            return;
        } else {
            if (inode->blocks[NDIRECT] == 0) {
                inode->blocks[NDIRECT] = this->bm->alloc_block();
            }
            char blockid_buf[BLOCK_SIZE];
            this->bm->read_block(inode->blocks[NDIRECT], blockid_buf);
            ((blockid_t *) blockid_buf)[index - NDIRECT] = this->bm->alloc_block();
            this->bm->write_block(inode->blocks[NDIRECT], blockid_buf);
        }
    }
}


void
inode_manager::free_block_by_index(inode_t *inode, uint32_t index) {
    if (index < NDIRECT) {
        this->bm->free_block(inode->blocks[index]);
        return;
    }
    this->bm->free_block(get_blockid_by_index(inode, index));
}

blockid_t
inode_manager::get_blockid_by_index(inode_t *inode, uint32_t index) {
    if (index < NDIRECT) {
        return inode->blocks[index];
    } else {
        if (inode->blocks[NDIRECT] == 0) {
            return 0;
        }
        char buf[BLOCK_SIZE];
        this->bm->read_block(inode->blocks[NDIRECT], buf);
        return ((blockid_t *) buf)[index - NDIRECT];
    }
}


/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size) {
    /*
     * your code goes here.
     * note: read blocks related to inode number inum,
     * and copy them to buf_out
     */
    inode_t *inode = this->get_inode(inum);
    if (!inode) {
      return;
    }
    char buf[BLOCK_SIZE];
    // inode_t *ino = get_inode(inum);
    // ino->atime = time(0);
    // int num_block = 0;
    // int size_remain = 0;
    // //ino == null
    // if(!ino) {
    //   return;
    // }
    // *size = ino->size;
    // //不嫩那个size < maxfile * nlock_size
    // if(!((unsigned int) * size <= MAXFILE * BLOCK_SIZE)) {
    //   return;
    // }
    // num_block = *size / BLOCK_SIZE;
    // size_remain = *size % BLOCK_SIZE;
    // *buf_out = (char *)malloc(*size);
    // int i;
    // //从0到num_block
    // for(i = 0; i < num_block; i++){
    //   api_read_get_block_helper(ino, i, *buf_out, BLOCK_SIZE);
    // }
    // //remain size
    // if(size_remain){
    //   api_read_get_block_helper(ino, i, *buf_out, size_remain);
    // }
    // free(ino);
    // return;
    *size = inode->size;
    *buf_out = (char *) malloc(*size);
    int block_count;
    if(inode->size == 0) {
      block_count = 0;
    } else {
      block_count = (inode->size - 1) / BLOCK_SIZE + 1;
    }
    for (int i = 0; i < block_count; i++) {
        bm->read_block(get_blockid_by_index(inode, i), buf);
        if (i == block_count - 1 && (inode->size) % BLOCK_SIZE) {
            memcpy(*buf_out + BLOCK_SIZE * i, buf, (inode->size) % BLOCK_SIZE);
        } else {
            memcpy(*buf_out + BLOCK_SIZE * i, buf, BLOCK_SIZE);
        }
    }
    return;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size) {
    /*
     * your code goes here.
     * note: write buf to blocks of inode inum.
     * you need to consider the situation when the size of buf
     * is larger or smaller than the size of original inode
     */
  //   inode_t *inode = get_inode(inum); 
  // //先排除一些边界情况
  // if(!(size >= 0 && (uint32_t)size <= MAXFILE * BLOCK_SIZE)) {
  //   return;
  // }
  // if(!inode) {
  //   printf("\tinode == NULL\n");
  //   return;
  // }
  // if(!((unsigned int)size <= MAXFILE * BLOCK_SIZE)) {
  //   return;
  // }
  // if(!(inode->size <= MAXFILE * BLOCK_SIZE)) {
  //   return;
  // }
  // inode->atime = time(0);
	// inode->mtime = time(0);
	// inode->ctime = time(0);
  // int num_block = 0;
  // int size_remain = 0;
  // int oldBlocks, newBlocks;
  // char temp[BLOCK_SIZE];
  
  // //获取oldBlocks
  // if(inode->size == 0) {
  //   oldBlocks = 0;
  // } else {
  //   oldBlocks = (inode->size - 1)/BLOCK_SIZE + 1;
  // }
  // //获取newBlocks
  // if(size == 0) {
  //   newBlocks = 0;
  // } else {
  //   newBlocks = (size - 1) / BLOCK_SIZE + 1;
  // }
  // if(newBlocks > int (NDIRECT + NINDIRECT)) {
  //   return;
  // }
  // if(oldBlocks < newBlocks) {
  //   printf("\tnow is new more old\n");
  //   for(int j = oldBlocks; j < newBlocks; j++) {
  //     if(inode == NULL) {
  //       printf("\tinfo: inode is NULL\n");
  //       exit(0);
  //     } else {
  //       /* code */
  //     }
  //     char buf[BLOCK_SIZE];
  //     // printf("\tinfo: alloc_block_helper %d\n", n);
  //     if(j < NDIRECT) {
  //       inode->blocks[j] = bm->alloc_block();
  //     } else if(j < MAXFILE) {
  //       if(!inode->blocks[NDIRECT]){
  //         printf("\tinfo: new NINDIRECT!\n");
  //         inode->blocks[NDIRECT] = bm->alloc_block();
  //       }
  //       bm->read_block(inode->blocks[NDIRECT], buf);      
  //       ((blockid_t *)buf)[j - NDIRECT] = bm->alloc_block();
  //       bm->write_block(inode->blocks[NDIRECT], buf); 
  //     } else {
  //       printf("\tinfoout of range\n");
  //       exit(0);
  //     }
  //   }
  // } else if(oldBlocks > newBlocks) {
  //   printf("\tnow is old more new\n");
  //   for(int j = newBlocks; j < oldBlocks; j++) {
  //     bm->free_block(get_blockid_helper(inode, j));
  //   }
  // }

  // num_block = size / BLOCK_SIZE;
  // size_remain = size % BLOCK_SIZE;
  // int i;
  // for(i = 0; i < num_block; i++) {
  //   bm_wrt_helper(inode, i, buf + BLOCK_SIZE * i);
  // }
  // if(size_remain){
  //   memcpy(temp, buf + i * BLOCK_SIZE, size_remain);
  //   bm_wrt_helper(inode, i, temp);
  // }
  // update_it(inode, 1000, size, 1);
  // put_inode(inum, inode);
  // free(inode);
  // return;
    inode_t *inode = this->get_inode(inum);
    if (!inode) {
      return;
    }
    int block_size_before;
    int block_size_after;
    // //获取oldBlocks
    if(inode->size == 0) {
      block_size_before = 0;
    } else {
      block_size_before = (inode->size - 1)/BLOCK_SIZE + 1;
    }
    //获取newBlocks
    if(size == 0) {
      block_size_after = 0;
    } else {
      block_size_after = (size - 1) / BLOCK_SIZE + 1;
    }
    if(block_size_after > int (NDIRECT + NINDIRECT)) {
      return;
    }
    int min_block = MIN(block_size_before, block_size_after);
    int max_block = MAX(block_size_after, block_size_before);

    if (block_size_after > block_size_before) {
        // need to alloc new block
        for (int i = min_block; i < max_block; ++i) {
          alloc_block_by_index(inode, i);
        }
    } else {
        // need to free outs block
        for (int i = min_block; i < max_block; ++i) {
          free_block_by_index(inode, i);
        }
    }
    char temp[BLOCK_SIZE];
    for (int i = 0; i < block_size_after; ++i) {
        if (i == block_size_after - 1 && size % BLOCK_SIZE) {
            memcpy(temp, buf + i * BLOCK_SIZE, size % BLOCK_SIZE);
            bm->write_block(get_blockid_by_index(inode, i), temp);
        } else {
            bm->write_block(get_blockid_by_index(inode, i), buf + i * BLOCK_SIZE);
        }
    }

    inode->size = size;
    uint32_t t = (unsigned int) time(NULL);
    inode->atime = t;
    inode->ctime = t;
    inode->mtime = t;
    put_inode(inum, inode);
    free(inode);
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  inode_t * inode = get_inode(inum);
  //当inode不为空时
  if(inode != NULL) {
    a.atime = inode->atime;
    a.mtime = inode->mtime;
    a.ctime = inode->ctime;
    a.type = inode->type;
    a.size = inode->size;
    free(inode);
  } else {
    /* code */
    printf("\tinode is NULL now in get_attr\n");
  }
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  // printf("\tinfo: remove_file %d\n", inum);
  inode_t *inode = get_inode(inum);
  if(!inode) {
    return;
  }
  //get block_num
  int block_num;
  if(inode->size == 0) {
    block_num = 0;
  } else {
    block_num = (inode->size - 1)/BLOCK_SIZE + 1;
  }
  //当block——num小于NDI时
  if(block_num <= NDIRECT){
    printf("\tnow is less then NDI\n");
		for(uint32_t i= 0; i< uint32_t(block_num); i++){
			bm->free_block(inode->blocks[i]);
		}
	}else{
    //当block_num大于NDI的时候
    printf("\tnow is more than NDI\n");
    blockid_t tempBlock[NINDIRECT];
		for(uint32_t i= 0; i< NDIRECT; i++){
			bm->free_block(inode->blocks[i]);
		}
		bm->read_block(inode->blocks[NDIRECT], (char*) &tempBlock);
		for(uint32_t i= NDIRECT; i< uint32_t(block_num); i++){
			bm->free_block(tempBlock[i-NDIRECT]);
		}
		bm->free_block(inode->blocks[NDIRECT]);
	}
	free_inode(inum);
	free(inode);
  return;
}

void
inode_manager::alloc_inode_by_inum(uint32_t type, uint32_t inum) {

    inode_t *inode;
    inode = get_inode(inum);
    if (inode != NULL) {
        return;
    } else {
        inode_t *ino;
        ino = (inode_t *) malloc(sizeof(inode_t));
        bzero(ino, sizeof(inode_t));
        ino->type = type;
        ino->size = 0;
        unsigned int t = time(NULL);
        ino->atime = t;
        ino->mtime = t;
        ino->ctime = t;
        put_inode(inum, ino);
        free(ino);
        return;
    }
}
