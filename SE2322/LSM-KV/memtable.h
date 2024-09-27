#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include "strucneed.h"

#define MAXSIZE 2086880

using namespace std;


class MemTable
{
public:
    Node *head;
    size_t memTableSize;
    uint64_t nodeNum;
    MemTable();
    ~MemTable();
    size_t size();
    string get(uint64_t &key);
    bool put(uint64_t &key, const string &val);
    bool remove(uint64_t &key);
    void clear();
    Node* keyValue();
    bool empty();
};
