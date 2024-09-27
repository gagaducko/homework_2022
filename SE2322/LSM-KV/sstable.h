#pragma once

#include "strucneed.h"
#include <vector>
#include "memtable.h"
#include <iostream>
#include <cstdint>
#include <fstream>
#include "MurmurHash3.h"
#include "utils.h"

using namespace std;

class SSTable
{
public:
    Header header;
    bool BF[10240];
    Index *index;
    string filePath;
    uint32_t totalSize;
    void setBF(uint64_t key);
    ofstream creatWrite(string &path);
    SSTable(string fp);
    SSTable(MemTable &memt, uint64_t &tStamp, uint64_t &fileNum, string &dirName, uint64_t level = 0);
    SSTable(vector<keyValue> &nodes, uint64_t tStamp, uint64_t &fileNum, string &dirName, uint32_t level);
    ~SSTable();
    bool isExist(uint64_t key);
    string get(uint64_t key);
    uint64_t getTimeStamp();
    uint64_t getMinKey();
    uint64_t getMaxKey();
    uint64_t getPairNum();
    uint64_t getIndexKey(uint64_t i);
    void rmFile();
};

