#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    sstHandler.setDirName(dir);
}

KVStore::~KVStore()
{
    sstHandler.toSST(memtable);
    memtable.clear();
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(!memtable.put(key, s)) {
        sstHandler.toSST(memtable);
        memtable.clear();
        memtable.put(key, s);
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string s = memtable.get(key);
    if(s == "") {
        s = sstHandler.get(key);
    }
    if(s == DEL) {
        return "";
    } else {
        return s;
    }
	return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    string tmp = memtable.get(key);
    if(tmp == DEL) {
        return false;
    }
    string tmp2 = sstHandler.get(key);
    if(((tmp2 != "") && tmp2 != DEL) || (tmp != "")) {
        string tmp3 = DEL;
        memtable.remove(key);
        put(key, tmp3);
        return true;
    }
    memtable.remove(key);
    string tmp3 = DEL;
    put(key, tmp3);
	return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memtable.clear();
    sstHandler.clear();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{	
    for(uint64_t k = key1; k <= key2; k++) {
        list.push_back(make_pair(k, get(k)));
    }
}
