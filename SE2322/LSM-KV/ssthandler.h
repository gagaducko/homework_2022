#pragma once

#include "sstable.h"
#include <iostream>
#include "strucneed.h"
#include <vector>
#include <string>
#include <queue>
#include "utils.h"
#include <fstream>
#include <regex>
#include <cmath>
#include <algorithm>

using namespace std;

class SSTHandler
{
public:
    vector<vector<SSTable*>> sstHandlerList;
    uint64_t tStamp;
    uint64_t fileNum;
    string dirName;
    SSTHandler();
    ~SSTHandler();
    bool checkCompact(uint32_t level);
    void readFiles();
    void compactKernel(priority_queue<tsKeyIndex> &nodeq, uint32_t level, uint64_t ts, bool lastlevel = false);
    bool isExist(uint64_t key);
    string get(uint64_t key);
    void toSST(MemTable &memt);
    void setDirName(const std::string &dirname);
    void clear();
    //compaction
    void compaction(uint32_t level);
    void compactionStep1(vector<uint64_t> &indexList, uint32_t &level);
    void compactionStep2(uint64_t &minKey, uint64_t &maxKey, uint64_t &maxTimeStamp, vector<uint64_t> &indexList, uint32_t &level);
    void compactionStep3(uint64_t &minKey, uint64_t &maxKey, uint64_t &maxTimeStamp, vector<uint64_t> &indexList, uint32_t &level);
    void compactionStep4(uint32_t &level);
    void compactionMergeSort(vector<uint64_t> &indexList, priority_queue<tsKeyIndex> &nodeq, uint32_t &level);
    void compactionMakeNewSST(vector<uint64_t> &indexList, uint32_t &level);
    uint64_t getFileN(string fileName);
};
