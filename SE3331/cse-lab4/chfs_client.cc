// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

chfs_client::chfs_client(std::string extent_dst)
{
    ec = new extent_client(extent_dst);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    //no
    // return ! isfile(inum);
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 

    return false;
}


//partB
// bool
// chfs_client::issymlink(inum inum)
// {
//     extent_protocol::attr a;

//     if (ec->getattr(inum, a) != extent_protocol::OK) {
//         printf("error getting attr\n");
//         return false;
//     }

//     if (a.type == extent_protocol::T_SYMLINK) {
//         printf("isfile: %lld is a symlink\n", inum);
//         return true;
//     } 

//     return false;
// }

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
//partB
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    string buf;
    r = ec->get(ino, buf);

    if(r != OK) {
        printf("\terror in setattr\n");
        return r;
    }

    //change buf的大小
    buf.resize(size);

    r = ec->put(ino, buf);

    if(r != OK) {
        printf("\terror in setattr\n");
        return r;
    }

    return r;
}


//in order to make create and mkdir more easy
int
chfs_client::create_mkdir_func(inum parent, const char *name, mode_t mode, inum &ino_out, int type) {
    int r = OK;
    bool found = false;
	r = lookup(parent, name, found, ino_out);
	if (found) {
        printf("\tfile has already existed\n");
        return r;
    }
	
	//create the file
    //1 is the create and 2 is the mkdir
    if(type == 1) {
        ec->create(extent_protocol::T_FILE, ino_out);
    } else if(type == 2) {
        ec->create(extent_protocol::T_DIR, ino_out);
    }

	//add an entry into parent
	std::string buf;
	r = ec->get(parent, buf);

    if (r != OK) {
        printf("create error\n");
        return r;
    }

	buf.append(string(name) + ":" + filename(ino_out) + "/");

	r = ec->put(parent, buf);
    if (r != OK) {
        printf("create error\n");
        return r;
    }

    return r;
}

//partB
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    return create_mkdir_func(parent, name, mode, ino_out, 1);
}

//partB
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    return create_mkdir_func(parent, name, mode, ino_out, 2);
}

//partB
int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    found = false;
    //look parent is exist
    if (!isdir(parent)) {
        return r;
    }

    //get parent dir
    std::list<chfs_client::dirent> entries;
    r = chfs_client::readdir(parent, entries);
    if (r != OK) {
        printf("\tlookup is wrong\n");
        return r;
    }

    //by name ,lookup file
    std::list<chfs_client::dirent>::iterator it = entries.begin();
    while (it != entries.end()) {
        string filename = it->name;
        if (filename == std::string(name)) {
            found = true;
            ino_out = it->inum;
            r = EXIST;
            return r;
        }
        it++;
    }

    return r;
}


//partB
int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    //用于读取一个文件夹中的文件
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    string buf;
	r = ec->get(dir, buf);
    if (r != OK) {
        printf("Wrong in readdir\n");
        return r;
    }
	// 格式: "name:inum/name:inum/name:inum/"
	int name_start = 0;
	int name_end = buf.find(':');
	while (name_end != string::npos) {
	//获取文件 name 和 inum
		string name = buf.substr(name_start, name_end - name_start);
		int inum_start = name_end + 1;
		int inum_end = buf.find('/', inum_start);
		string inum = buf.substr(inum_start, inum_end - inum_start);
		struct dirent entry;
		entry.name = name;
		entry.inum = n2i(inum);
		list.push_back(entry);
		name_start = inum_end + 1;
		name_end = buf.find(':', name_start);
	};
    return r;
}


//partB
int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    string buf;
    ec->get(ino, buf);
    //off超出范围的话
    if(off > buf.size()) {
        data = std::string("");
        return r;
    } 
    //如果只是部分超出了范围的话
    if((off + size) > buf.size()) {
        data = buf.substr(off);
        return r;
    }
    //此外的正常情况
    data = buf.substr(off, size);
    return r;
}


//partB
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    string buf;
	r = ec->get(ino, buf);

    if (r != OK) {
        printf("Wrong in write\n");
        return r;
    }
	
	if (off + size <= buf.size()) {
		for (int i = off; i < off + size; i++) buf[i] = data[i - off];
		bytes_written = size;
		ec->put(ino, buf);
		return r;
	};
	buf.resize(off + size); 
	for (int i = off; i < off + size; i++) buf[i] = data[i - off];
	bytes_written = size;
	ec->put(ino, buf);

    return r;
}


//partB
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found = false;
	inum inum;
	r = lookup(parent, name, found, inum);
    if (!found) {
        printf("file doesn't exist\n");
        r = NOENT;
        return r;
    }

    //remove the file
	ec->remove(inum);
	
	/* update parent directory content */
	std::string buf;
	r = ec->get(parent, buf);
    if (r != OK) {
        printf("\tcreate error\n");
        return r;
    }
	int erase_start = buf.find(name);
	int erase_after = buf.find('/', erase_start);
	buf.erase(erase_start, erase_after - erase_start + 1);

	r = ec->put(parent, buf);
    if (r != OK) {
        printf("\tcreate error\n");
        return r;
    }

    return r;
}



// int chfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out)
// {
//     int r = OK;

//     // check if symlink name has existed
//     bool found = false;
//     inum tmp;
//     lookup(parent, name, found, tmp);
//     if (found) {
//         // has existed
//         return EXIST;
//     }

//     // pick a inum
//     // init the symlink
//     ec->create(extent_protocol::T_SYMLINK, ino_out);
//     ec->put(ino_out, std::string(link));

//     //向parent添加一个entry
//     std::string buf;
//     ec->get(parent, buf);
//     buf.append(std::string(name) + ":" + filename(ino_out) + "/");
//     ec->put(parent, buf);
//     return r;
// }

// int chfs_client::readlink(inum ino, std::string &data)
// {
//     int r = OK;
    
//     string buf;
//     r = ec->get(ino, buf);

//     if(r != OK) {
//         printf("readlink error\n");
//         return r;
//     }

//     data = buf;
    
//     return r;
// }
