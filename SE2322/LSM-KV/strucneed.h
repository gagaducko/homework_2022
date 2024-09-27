#ifndef STRUCNEED_H
#define STRUCNEED_H

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>

using namespace std;

struct Node {
    Node * right;
    Node * down;        //有向下和向右就可以了
    uint64_t key;
    string val;
    Node(): right(nullptr), down(nullptr) {}
    Node(Node *right, Node *down, uint64_t key, string val)
            : right(right),
              down(down),
              key(key),
              val(val) {}
};

struct Header {                //header
    uint64_t minKey;
    uint64_t maxKey;
    uint64_t pairNum;
    uint64_t timeStamp;
};

struct Index{                   //index(key and offset)
    uint64_t key;
    uint32_t offset;
};

struct keyValue {               //key and value
    uint64_t key;
    string val;
    keyValue(uint64_t k, string v) :
        key(k),
        val(v){}
};

struct tsMinIndex {
    uint64_t ts;  // time stamp
    uint64_t min;  // minimal key
    uint64_t index;  // index of this element in the handler
    tsMinIndex(uint64_t ts, uint64_t min, uint64_t index) : ts(ts), min(min), index(index) {}
    bool operator < (const tsMinIndex &b) const {
        if (ts == b.ts)
            return min > b.min;
        return ts > b.ts;
    }
};

struct tsKeyIndex {
    uint64_t key;  // key
    uint64_t timestamp;  // timestamp of the sstable, where the key exist
    uint32_t level;  // sstable level
    uint64_t index;  // index of the sstable, where the key exist
    tsKeyIndex() {}
    tsKeyIndex(uint64_t key, uint64_t timestamp, uint32_t level, uint64_t index) : key(key), timestamp(timestamp), level(level), index(index) {}
    bool operator < (const tsKeyIndex &b) const {
        if (key == b.key)
            return timestamp < b.timestamp;
        return key > b.key;
    }
};



#endif // STRUCNEED_H
