#pragma once

#include "kvstore_api.h"
#include "memtable.h"
#include "sstable.h"
#include "ssthandler.h"
#include <iostream>

#define DEL "~DELETED~"

using namespace std;

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    MemTable memtable;
    SSTHandler sstHandler;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
