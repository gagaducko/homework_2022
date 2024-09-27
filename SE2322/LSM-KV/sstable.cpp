#include "sstable.h"

///////////////////////////////////
/// \brief SSTable::SSTable
/// \param fp
/// 由一个file创建sstable
///////////////////////////////////
SSTable::SSTable(string fp)
{
    //set filepath
    filePath = fp;
    ifstream file(filePath, ios::binary);
    if(!file.is_open()) {
        cerr << "read: can not open " << filePath << endl;
        exit(-1);
    }
    //设置header
    file.read(reinterpret_cast<char*>(&header), sizeof(header));
    //设置bloom filter
    file.read(reinterpret_cast<char*>(BF), sizeof(BF));
    //设置index
    index = new Index[header.pairNum];
    for(uint64_t i = 0; i < header.pairNum; i++) {
        file.read(reinterpret_cast<char*>(&index[i]), sizeof(uint64_t) + sizeof(uint32_t));
    }
    //设置totalSize
    file.seekg(0, ios::end);
    totalSize = file.tellg();
    file.close();
}


//////////////////////////
/// \brief SSTable::SSTable
/// 由一个memtable创建sstable
///////////////////////////
SSTable::SSTable(MemTable &memt, uint64_t &tStamp, uint64_t &fileNum, string &dirName, uint64_t level)
{
    // set filename
    filePath = dirName + "/level-" + to_string(level) + "/" + to_string(fileNum++) + ".sst";
    // set header
    Node *keyVal = memt.keyValue()->right;
    header.timeStamp = tStamp++;
    header.pairNum = memt.size();
    header.minKey = keyVal->key;
    // set index
    Node *tmp = keyVal;
    uint64_t i = 0;
    uint32_t offset = sizeof(header) + sizeof(BF) + (sizeof(uint64_t) + sizeof(uint32_t)) * header.pairNum;
    index = new Index[header.pairNum];
    // find the max key and construct index and bloom filter
    while (tmp != nullptr) {
        header.maxKey = tmp->key;
        index[i].offset = offset;
        index[i].key = tmp->key;
        offset += tmp->val.length();
        setBF(tmp->key);
        tmp = tmp->right;
        i++;
    }
    totalSize = offset;
    //写入文件
    string path = "data/level-" + std::to_string(level);
    ofstream file = creatWrite(path);
    //写入data部分
    tmp = keyVal;
    while (tmp != nullptr) {
    file.write((char*)((tmp->val).data()), sizeof(char) * tmp->val.length());
        tmp = tmp->right;
    }
    file.close();
}

ofstream SSTable::creatWrite(string &path)
{
    //创建、打开、写入
    if(!utils::dirExists(path))
        utils::mkdir((char*)(path.data()));
    ofstream file(filePath, ios::binary);
    if (!file.is_open()) {
        std::cerr << "write: can not open " << filePath << endl;
        exit(-1);
    }
    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(reinterpret_cast<char*>(BF), sizeof(BF));
    for (uint64_t i = 0; i < header.pairNum; i++)
        file.write(reinterpret_cast<char*>(&index[i]), sizeof(uint64_t) + sizeof(uint32_t));
    return file;
}



/////////////////////////////
/// \brief SSTable::SSTable
/// 由一个nodelist来创建sstable
//////////////////////////////
SSTable::SSTable(vector<keyValue> &nodes, uint64_t tStamp, uint64_t &fileNum, string &dirName, uint32_t level)
{
    // set filename
    filePath = dirName + "/level-" + to_string(level) + "/" + to_string(fileNum++) + ".sst";
    // set header
    header.minKey = (*nodes.begin()).key;
    header.maxKey = (*(nodes.end()-1)).key;
    header.timeStamp = tStamp;
    header.pairNum = nodes.size();
    // set index
    uint64_t i = 0;
    uint32_t offset = sizeof(header) + sizeof(BF) + (sizeof(uint64_t) + sizeof(uint32_t)) * header.pairNum;
    index = new Index[header.pairNum];
    for (std::vector<keyValue>::iterator it = nodes.begin(); it != nodes.end(); it++) {
        index[i].key = (*it).key;
        index[i].offset = offset;
        offset += (*it).val.length();
        setBF((*it).key);
        i++;
    }
    // set total size of the data
    totalSize = offset;
    //写入文件
    string path = "data/level-" + std::to_string(level);
    ofstream file = creatWrite(path);
    //写入data部分
    for (vector<keyValue>::iterator it = nodes.begin(); it != nodes.end(); it++)
        file.write((char*)((*it).val.data()), sizeof(char) * (*it).val.length());
    file.close();
}

SSTable::~SSTable()
{
    delete [] index;
}

//////////////////////////
/// \brief SSTable::setBF
/// \param key
/// set a bloom filter
///////////////////////
void SSTable::setBF(uint64_t key)
{
    unsigned int hash[4] = { 0 };
    MurmurHash3_x64_128(&key, sizeof (key), 1, hash);
    for(int i = 0; i < 4; i++) {
        BF[hash[i] % 10240] = true;
    }
}

//////////////////////////////
/// \brief SSTable::isExist
/// \param key
/// \return
/// is Exist
/////////////////////////////
bool SSTable::isExist(uint64_t key)
{
    uint64_t left = 0;
    uint64_t right = header.pairNum - 1;
    uint64_t middle = 0;
    unsigned int hash[4] = { 0 };
    MurmurHash3_x64_128(&key, sizeof (key), 1, hash);
    int flag = false;
    for(int i = 0; i < 4; i++) {
        if(!BF[hash[i] % 10240]) {
            flag = true;
        }
    }
    if(flag == true) {
        return false;
    }
    //如果存在的话search一下
    while(right >= left) {
        middle = left + (right - left) / 2;
        if(index[middle].key == key) {
            return true;
        }
        if(index[middle].key > key) {
            if(middle <= 0) {
                break;
            }
            right = middle - 1;
        } else {
            left = middle + 1;
        }
    }
    return false;
}

////////////////////////
/// \brief SSTable::get
/// \param key
/// \return
/// find the key
////////////////////////
string SSTable::get(uint64_t key)
{
    uint64_t left = 0;
    uint64_t right = header.pairNum - 1;
    uint64_t middle = 0;
    uint32_t offset;
    bool flag = false;
    while(left <= right) {
        middle = left + (right - left) / 2;
        if(index[middle].key == key) {
            offset = index[middle].offset;
            flag = true;
            break;
        }
        if(index[middle].key > key) {
            if(middle <= 0) {
                break;
            }
            right = middle - 1;
        } else {
            left = middle + 1;
        }
    }
    //如果没找到
    if(!flag) {
        return "";
    }
    //找到了就打开文件并读值
    string value;
    ifstream file(filePath, ios::binary);
    if(!file.is_open()) {
        cerr << "read: can not open " << filePath << std::endl;
        exit(-1);
    }
    file.seekg(offset);
    uint32_t numChar;
    if(middle ==header.pairNum - 1) {
        numChar = totalSize - offset;
    } else {
        numChar = index[middle + 1].offset - offset;
    }

    char tmp;
    for(uint32_t i = 0; i < numChar; i++) {
        file.read(&tmp, sizeof (tmp));
        value += tmp;
    }
    file.close();
    return value;
}

uint64_t SSTable::getTimeStamp()
{
    return header.timeStamp;
}

uint64_t SSTable::getMinKey()
{
    return header.minKey;
}

uint64_t SSTable::getMaxKey()
{
    return header.maxKey;
}

uint64_t SSTable::getPairNum()
{
    return header.pairNum;
}

uint64_t SSTable::getIndexKey(uint64_t i)
{
    return index[i].key;
}

void SSTable::rmFile()
{
    utils::rmfile((char*)(filePath.data()));
}

