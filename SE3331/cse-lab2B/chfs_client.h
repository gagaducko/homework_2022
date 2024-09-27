#ifndef chfs_client_h
#define chfs_client_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "extent_client.h"
#include <vector>


//lab2A changed
//20221027
#define min(a, b) (a < b) ? a : b
#define max(a, b) (a > b) ? a : b

class chfs_client {
  extent_client *ec;
  lock_client *lc;
 public:

    typedef unsigned long long inum;
    enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
    typedef int status;

    struct fileinfo {
        unsigned long long size;
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    struct dirinfo {
        unsigned long atime;
        unsigned long mtime;
        unsigned long ctime;
    };
    typedef struct dirent {
        std::string name;
        chfs_client::inum inum;
    } dirent_t ;

private:
    static std::string filename(inum);
    static inum n2i(std::string);

    

 public:
  chfs_client(std::string, std::string);

    bool isfile(inum);
    bool isdir(inum);

    void LOCK(inum);
	void UNLOCK(inum);
    
    int getfile(inum, fileinfo &);
    int getdir(inum, dirinfo &);

    int setattr(inum, size_t);
    int lookup(inum, const char *, bool &, inum &);
    int create(inum, const char *, mode_t, inum &);
    int readdir(inum, std::list<dirent> &);
    int write(inum, size_t, off_t, const char *, size_t &);
    int read(inum, size_t, off_t, std::string &);
    int unlink(inum,const char *);
    int mkdir(inum , const char *, mode_t , inum &);
    int create_mkdir_func(inum , const char *, mode_t , inum &, int);
    
  /** you may need to add symbolic link related methods here.*/
  bool issymlink(inum);
  int link(inum parent, const char* name, const char* link, inum& ino_out);
  int readlink(inum ino, std::string &data);
};

#endif 
