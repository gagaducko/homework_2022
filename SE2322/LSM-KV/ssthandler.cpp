#include "ssthandler.h"

SSTHandler::SSTHandler()
{
    fileNum = 0;
    tStamp = 1;
    vector<SSTable *> firstElem;
    sstHandlerList.push_back(firstElem);
}

SSTHandler::~SSTHandler()
{
    vector<vector<SSTable*>>::iterator it;
    vector<SSTable*>::iterator sit;
    for (it = sstHandlerList.begin(); it != sstHandlerList.end(); it++) {
        for (sit = (*it).begin(); sit != (*it).end(); sit++)
            delete *sit;
    }
}

bool SSTHandler::checkCompact(uint32_t level)
{
    level++;
    uint64_t check = pow(2, level);
    if(sstHandlerList[level].size() <= check) {
        return false;
    }
    return true;
}

uint64_t SSTHandler::getFileN(string fileName)
{
    smatch results;
    regex pattern("(\\d+).sst");
    regex_match(fileName, results, pattern);
    uint64_t ret = stoi(results.str(1));
    return ret;
}

void SSTHandler::readFiles()
{
    vector<string> dir;
    utils::scanDir(dirName, dir);
    if (dir.empty())
        return;
    // construct the sstable
    // get the max timestamp
    uint64_t curTimeStamp = 0;
    uint64_t curFileNum = 0;
    uint32_t level = 0;
    vector<string>::iterator it;
    for (it = dir.begin(); it != dir.end(); it++) {
        vector<string> files;
        utils::scanDir(dirName + "/" + *it, files);
        std::vector<SSTable*> temp;
        sstHandlerList.push_back(temp);
        vector<string>::iterator fit;
        for (fit = files.begin(); fit != files.end(); fit++) {
            SSTable *sst = new SSTable(dirName + "/" + *it + "/" + *fit);
            curFileNum = getFileN(*fit);
            curTimeStamp = sst->getTimeStamp();
            if(tStamp <= curTimeStamp) {
                tStamp = curTimeStamp;
            }
            if(fileNum <= curFileNum) {
                fileNum = curFileNum;
            }
            sstHandlerList[level].push_back(sst);
        }
        level++;
    }
    tStamp++;
    fileNum++;
}


void SSTHandler::compactKernel(priority_queue<tsKeyIndex> &nodeq, uint32_t level, uint64_t ts, bool lastlevel)
{
    tsKeyIndex cur, last = nodeq.top();
    vector<keyValue> newSST;
    size_t memSize = 0;
    while(!nodeq.empty()) {
        cur = nodeq.top();
        if ((cur.key != last.key) || (cur.timestamp >= last.timestamp)) {
            std::string value = sstHandlerList[cur.level][cur.index]->get(cur.key);
             if (!(lastlevel && value == "~DELETED~")) {
                if (memSize + sizeof(uint64_t) + sizeof(uint32_t) + value.length() > MAXSIZE) {
                    SSTable *tmp = new SSTable(newSST, ts, fileNum, dirName, level);
                    sstHandlerList[level].push_back(tmp);
                    memSize = 0;
                    newSST.clear();
                    continue;
                } else {
                    newSST.push_back(keyValue(cur.key, value));
                    memSize += sizeof(uint64_t) + sizeof(uint32_t) + value.length();
                }
            }
        }
        last = cur;
        nodeq.pop();
    }
    if(!newSST.empty()) {
        SSTable * tmp = new SSTable(newSST, ts, fileNum, dirName, level);
        sstHandlerList[level].push_back(tmp);
    }
}


bool SSTHandler::isExist(uint64_t key)
{
    vector<vector<SSTable*>>::iterator it;
    std::vector<SSTable*>::iterator sit;
    for (it = sstHandlerList.begin(); it != sstHandlerList.end(); it++) {
        for (sit = (*it).begin(); sit != (*it).end(); sit++) {
            if ((**sit).isExist(key))
                return true;
        }
    }
    return false;
}

string SSTHandler::get(uint64_t key)
{
    vector<SSTable *> tmp;
    vector<vector<SSTable *>>::iterator it;
    vector<SSTable*>::iterator sit;
    for(it = sstHandlerList.begin(); it != sstHandlerList.end(); it++) {
        for (sit = (*it).begin(); sit != (*it).end(); sit++) {
            if((**sit).isExist(key)) {
                tmp.push_back(*sit);
            }
        }
    }
    if(tmp.empty()) {
        return "";
    }
    SSTable * newest = *(tmp.begin());
    std::vector<SSTable*>::iterator i;
    for (i = tmp.begin(); i != tmp.end(); i++)
        if ((*i)->getTimeStamp() > newest->getTimeStamp())
            newest = (*i);
    return newest->get(key);
}

void SSTHandler::toSST(MemTable &memt)
{
    if(memt.empty()) {
        return;
    }
    SSTable * tmp = new SSTable(memt, tStamp, fileNum, dirName);
    sstHandlerList[0].push_back(tmp);
    if(sstHandlerList[0].size() > 2) {
        compaction(0);
    }
}

void SSTHandler::setDirName(const std::string &dirname)
{
    dirName = dirname;
    if (utils::dirExists(dirName)) {
        readFiles();
    } else {
        utils::mkdir((char*)(dirName.data()));
    }
}

void SSTHandler::clear()
{
    sstHandlerList.clear();
    tStamp = 0;
    if(utils::dirExists(dirName)) {
        vector<string> dir;
        utils::scanDir(dirName, dir);
        vector<string>::iterator it;
        vector<string>::iterator fit;
        for (it = dir.begin(); it != dir.end(); it++) {
            vector<string> files;
            utils::scanDir(dirName + "/" + *it, files);
            for (fit = files.begin(); fit != files.end(); fit++) {
                utils::rmfile((char*)((dirName + "/" + *it + "/" + *fit).data()));
            }
            utils::rmdir((char*)((dirName + "/" + *it).data()));
        }
        utils::rmdir((char*)(dirName.data()));
    }
    vector<SSTable*> firstElem;
    sstHandlerList.push_back(firstElem);
}

////////////////////////////////////
/// \brief SSTHandler::compactionStep1
/// compaction step1:
///
/// first need to find the index of current level sstable
//////////////////////////////////////
void SSTHandler::compactionStep1(vector<uint64_t> &indexList, uint32_t &level)
{
    //如果是0，那就需要所有的sstable
    if (level == 0) {
        for(int i = 0; i < 3; i++) {
            indexList.push_back(i);
        }
    } else {  //如果level不是0
        uint64_t i = 0;
        priority_queue<tsMinIndex> q;
        vector<SSTable*>::iterator it;
        for (it = sstHandlerList[level].begin(); it != sstHandlerList[level].end(); it++, i++) {
            q.push(tsMinIndex((*it)->getTimeStamp(), (*it)->getMinKey(), i));
        }
        uint32_t sstNum = sstHandlerList[level].size() - std::pow(2, level + 1);
        tsMinIndex tmp = q.top();
        while (sstNum != 0) {
            tmp = q.top();
            indexList.push_back(tmp.index);
            q.pop();
            sstNum--;
        }
        while (!q.empty() && tmp.ts == q.top().ts) {
            tmp = q.top();
            indexList.push_back(tmp.index);
            q.pop();
        }
    }
}

////////////////////////////////////
/// \brief SSTHandler::compactionStep2
/// \param level
/// 找到覆盖键的区间，以及当前level最大的timeStamp
/// //////////////////////////////////
void SSTHandler::compactionStep2(uint64_t &minKey, uint64_t &maxKey, uint64_t &maxTimeStamp, vector<uint64_t> &indexList, uint32_t &level)
{
    minKey = sstHandlerList[level][*(indexList.begin())]->getMinKey();
    maxKey = sstHandlerList[level][*(indexList.begin())]->getMaxKey();
    maxTimeStamp = sstHandlerList[level][*(indexList.begin())]->getTimeStamp();
    vector<uint64_t>::iterator it ;
    for (it= indexList.begin(); it != indexList.end(); it++) {
        if(!(minKey < sstHandlerList[level][(*it)]->getMinKey())){
            minKey = sstHandlerList[level][(*it)]->getMinKey();
        }
        if(!(maxKey > sstHandlerList[level][(*it)]->getMaxKey())) {
            maxKey = sstHandlerList[level][(*it)]->getMaxKey();
        }
        if(!(maxTimeStamp > sstHandlerList[level][(*it)]->getTimeStamp())) {
            maxTimeStamp = sstHandlerList[level][(*it)]->getTimeStamp();
        }
    }
}


void SSTHandler::compactionMergeSort(vector<uint64_t> &indexList, priority_queue<tsKeyIndex> &nodeq, uint32_t &level)
{
    for (vector<uint64_t>::iterator it = indexList.begin(); it != indexList.end(); it++) {
        for (uint64_t i = 0; i < sstHandlerList[level][*it]->getPairNum(); i++) {
            nodeq.push(tsKeyIndex(sstHandlerList[level][*it]->getIndexKey(i), sstHandlerList[level][*it]->getTimeStamp(), level, *it));
        }
    }
}


void SSTHandler::compactionMakeNewSST(vector<uint64_t> &indexList, uint32_t &level)
{
    uint64_t k = 0;
    for (vector<uint64_t>::iterator it = indexList.begin(); it != indexList.end(); it++) {
        sstHandlerList[level][*it - k]->rmFile();
        delete sstHandlerList[level][*it - k];
        sstHandlerList[level].erase(sstHandlerList[level].begin() + *it - k);
        k++;
    }
}


///////////////////////////////////////
/// \brief SSTHandler::compactionStep3
/// 找下一个level SST的index来compact
/// merge sort
///////////////////////////////////////////
void SSTHandler::compactionStep3(uint64_t &minKey, uint64_t &maxKey, uint64_t &maxTimeStamp, vector<uint64_t> &indexList, uint32_t &level)
{
    uint32_t nextLevel = level + 1;
    priority_queue<tsKeyIndex> nodeq;
    if (sstHandlerList.size() > nextLevel) {  // next level exist
        vector<uint64_t> nextIndexList;
        uint64_t i = 0;
        for (vector<SSTable*>::iterator it = sstHandlerList[nextLevel].begin(); it != sstHandlerList[nextLevel].end(); it++, i++) {
            if (((*it)->getMinKey() >= minKey && (*it)->getMinKey() <= maxKey) && ((*it)->getMaxKey() >= minKey && (*it)->getMaxKey() <= maxKey)) {
               nextIndexList.push_back(i);
            }
        }
        // merge sort(x)
        compactionMergeSort(indexList, nodeq, level);
        for (vector<uint64_t>::iterator it = nextIndexList.begin(); it != nextIndexList.end(); it++) {
            for (uint64_t i = 0; i < sstHandlerList[nextLevel][*it]->getPairNum(); i++) {
                uint64_t tmpTimeStamp = sstHandlerList[nextLevel][*it]->getTimeStamp();
                nodeq.push(tsKeyIndex(sstHandlerList[nextLevel][*it]->getIndexKey(i), tmpTimeStamp, nextLevel, *it));
                if(!(maxTimeStamp > tmpTimeStamp)) {
                    maxTimeStamp = tmpTimeStamp;
                }
            }
        }
        //生成新的sstable并且写入
        compactKernel(nodeq, nextLevel, maxTimeStamp, nextLevel == (sstHandlerList.size() - 1));
        sort(indexList.begin(), indexList.end());
        compactionMakeNewSST(indexList, level);
        sort(nextIndexList.begin(), nextIndexList.end());
        compactionMakeNewSST(nextIndexList, nextLevel);
    } else {  // next level 不存在
        // merge sort(x)
        compactionMergeSort(indexList, nodeq, level);
        vector<SSTable*> newlevel;
        sstHandlerList.push_back(newlevel);  // new a level
        //生成新的sstable并且写入
        compactKernel(nodeq, level + 1, maxTimeStamp, true);
        sort(indexList.begin(), indexList.end());
        compactionMakeNewSST(indexList, level);
    }
}


/////////////////////////////////////
/// \brief SSTHandler::compactionStep4
/// \param level
/// 检查下一个level看是否需要compaction
///
/// /////////////////////////////////
void SSTHandler::compactionStep4(uint32_t &level)
{
    if (checkCompact(level + 1)) {
        compaction(level + 1);
    }
}


void SSTHandler::compaction(uint32_t level)
{
    vector<uint64_t> indexList;
    uint64_t minKey, maxKey, maxTimeStamp;
    //compaction step1
    compactionStep1(indexList, level);
    //compaction step2
    compactionStep2(minKey, maxKey, maxTimeStamp, indexList, level);
    //compaction step3
    compactionStep3(minKey, maxKey, maxTimeStamp, indexList, level);
    //compaction step4
    compactionStep4(level);
}
