#include "memtable.h"

MemTable::MemTable()
{
    head = new Node();
    nodeNum = 0;
    memTableSize = 0;
}

MemTable::~MemTable()
{
    clear();
}

////////////////////
/// \brief get
/// \param key
/// \return val
////////////////////
string MemTable::get(uint64_t &key)
{
    if(nodeNum == 0) {
        return "";
    }                           //没有num，就return“”
    Node *p = head;
    while(p) {
        while(p->right && p->right->key <= key) {           //p先向右
            if(p->right->key == key) {                      //如果值同了
                return p->right->val;                       //就return
            }
            p = p->right;
        }
        p = p->down;                                        //向右找不到就向下
    }
    return "";
}

///////////////////////////////
/// \brief put
/// \param key
/// \param val
/// \return true or false
///////////////////////////////
bool MemTable::put(uint64_t &key, const string &val)
{
    size_t nodeSize = sizeof (uint64_t) + sizeof (uint32_t) + val.length();
    vector<Node *> pathList;                                                        //路径记录
    Node *p = head;
    bool isExist = false;
    if((memTableSize + nodeSize) > MAXSIZE) {                                       //超出maxsize了就得采用sstable,return false
        return false;
    }
    while(p) {                                                                      //类似于get进行查找
        while(p->right && p->right->key < key) {
            p = p->right;
        }
        if(p->right && p->right->key == key) {
            p = p->right;
            isExist = true;
            break;
        }
        pathList.push_back(p);
        p = p->down;
    }

    //更新pair

    bool insertUp = true;
    Node * downNode = nullptr;
    //如果说isExist
    if(isExist) {
        size_t tmpSize = memTableSize - (p->val).length() + val.length();
        if(tmpSize > MAXSIZE) {             //先看有没有超
            return false;
        }
        while(p) {                          //将p的val更新了，并p=p->down
            p->val = val;
            p = p->down;
        }
        memTableSize = tmpSize;
        return true;
    }
    //如果说not exsit
    while(insertUp && pathList.size() > 0) {        //当路径pathsize存在
        Node * insertNode = pathList.back();        //返回路径的最后一个元素
        pathList.pop_back();
        insertNode->right = new Node(insertNode->right, downNode, key, val);
        downNode = insertNode->right;
        insertUp = (rand() & 1);                    //50%的概率
    }
    if(insertUp) {
        Node * oldNode = head;
        head  = new Node ();
        head->right = new Node (nullptr, downNode, key, val);
        head->down = oldNode;
    }
    memTableSize = memTableSize + nodeSize;
    nodeNum++;
    return true;
}



/////////////
/// \remove
/// \param key
/// \return true or false
/////////////
bool MemTable::remove(uint64_t &key)
{
    if(nodeNum == 0) {                  //先看看有没有node
        return false;
    }
    vector<Node *> pathList;            //依据记录轨迹
    Node * p = head;
    while(p) {                                            //类似put
        while(p->right && p->right->key < key) {
            p = p->right;
        }
        if(p->right && p->right->key == key) {
            pathList.push_back(p);
        }
        p = p->down;
    }

    //如果没有路径
    if(pathList.empty()) {
        return false;
    }
    //开删
    //删除 && link
    Node *temp;                     //tempNode
    Node * preNode;                 //preNode
    size_t nodeSize = sizeof (uint64_t) + sizeof (uint32_t) + pathList.back()->val.length();
    while(!pathList.empty()) {
        preNode = pathList.back();          //取最后一个元素
        if(preNode == head && head->right->right == nullptr) {
            head = head->down;
        }
        temp = preNode->right;
        preNode->right = temp->right;
        free(temp);
        pathList.pop_back();
    }
    memTableSize = memTableSize - nodeSize;
    nodeNum--;
    return true;
}

////////////////////
//clear
//
////////////////
void MemTable::clear()
{
    Node *curLevel = head;
    Node *nextLevel;
    while(curLevel) {
        nextLevel = curLevel->down;
        Node *temp;
        while(curLevel->right != nullptr) {
            temp = curLevel->right;
            curLevel->right = temp->right;
            free(temp);
        }
        free(curLevel);
        curLevel = nextLevel;
    }
    head = new Node();
    memTableSize = 0;
    nodeNum = 0;
}

//////////////////////////////
///
/// \brief MemTable::keyValue
/// \以一个Node返回所有的keyValue
///
//////////////////////////////
Node* MemTable::keyValue()
{
    Node * p = head;
    while(p->down != nullptr) {
        p = p->down;
    }
    return p;
}


//////////////////////////////
///
/// 返回现在memtable内node的数量
/// return nodeNum
///
/////////////////////////////
size_t MemTable::size()
{
    return nodeNum;
}



////////////////////////////////
/// \brief MemTable::empty
/// \return is empty?
///////////////////////////////
bool MemTable::empty()
{
    if(nodeNum == 0) {
        return true;
    } else {
        return false;
    }
}
