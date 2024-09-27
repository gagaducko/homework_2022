#include "ScapegoatTree.h"
#include <iostream>
#include <cmath>

int ScapegoatTree::GetRebalanceTimes()
{
    // TODO
    cout << balanceTimes << endl;
    return 0;
}

int times = 0;
void ScapegoatTree::Search(int key)
{
    Node * searNode = search(root, key, 0);
    if(searNode == nullptr) {
        cout << "Not Found" << endl;//nullptr
    } else {
        int val = searNode->nodeDepth;//nodeDepth
        cout << val << endl;
    }
}

//查找旧有node
Node * ScapegoatTree::searchHave(Node * node, int val, size_t & deep)
{
    if (node == nullptr) {
        return node;
    }
    if (node->value == val) {
        node->nodeDepth = deep;
        return node;
    }
    if (val < node->value) {
        deep++;
        return search(node->left, val, deep);
    }
    deep++;
    return search(node->right, val, deep);
}

void ScapegoatTree::Insert(int key)
{
    size_t new_node_depth = 0;
    Node * new_node = new Node(key);
    if(!contains(key)) {
       root = insertNode(root, new_node, new_node_depth);
       treeSize++;
    } else {
        new_node = searchHave(root, key, new_node_depth);
        new_node_depth = new_node->nodeDepth;
    }
    treeMaxSize = max(treeSize, treeMaxSize);//树的最大高度
    if ((new_node_depth) > hAlpha(treeSize, ALPHA)) {
        Node * scapegoat = findScapegoat(new_node, ALPHA);
        rebuild(scapegoat);//重构
    }
    return;
}

//void ScapegoatTree::Insert(int key)
//{
//    // TODO
//    if (contains(key)) {
//        size_t node_depth = 0;
////        size_t deep = getHeightNode(root, key, node_depth);
//        Node * keyNode = searchHave(root, key, node_depth);
//        node_depth = node_depth + 1;
//        if ((node_depth) > hAlpha(treeSize, ALPHA)) {
//    //        cout << "insert: " <<key << "  now it need balance" <<endl;
//            Node * scapegoat = findScapegoat(keyNode, ALPHA);//找到goal
//    //        cout << "the scapegoat is:" <<scapegoat->value <<endl;
//            rebuild(scapegoat);//重构
//        }
//        return;
//    }//有一样的直接return，不用插入了
//    size_t new_node_depth = 0;
//    Node * new_node = new Node(key);
//    root = insertNode(root, new_node, new_node_depth);//插入new node
//    treeSize++;//size
////    cout <<"insert the: << key <<"       the size is: " << treeSize << "     the depth is:" <<new_node_depth <<endl;
//    treeMaxSize = max(treeSize, treeMaxSize);//树的最大高度
//    if ((new_node_depth) > hAlpha(treeSize, ALPHA)) {
////        cout << "insert: " <<key << "  now it need balance" <<endl;
//        Node * scapegoat = findScapegoat(new_node, ALPHA);//找到goal
////        cout << "the scapegoat is:" <<scapegoat->value <<endl;
//        rebuild(scapegoat);//重构
//        if (scapegoat == root) {
//            treeMaxSize = treeSize;
//        }
//    }
////    cout <<"insert the: "<< key <<"       the size is: " << treeSize << "     the depth is:" <<new_node_depth <<endl;
////    seeWhat(root);
////    cout<<endl;
//    return;
//}


size_t ScapegoatTree::getHeightNode(Node * node, int key, size_t & deep){
    if (node->value == key) {
        return deep;
    }
    if (key < node->value) {
        deep++;
        return getHeightNode(node->left, key, deep);
    }
    deep++;
    return getHeightNode(node->right, key, deep);
}


//拍扁
Node * ScapegoatTree::flattenTree(Node * node, Node * head)
{
    if (node == nullptr) {
        return head;
    }
    node->right = flattenTree(node->right, head);
    return flattenTree(node->left, node);
}


//查找node
Node * ScapegoatTree::search(Node * node, int val, int deep)
{
    if (node == nullptr) {
        return node;
    }
    if (node->value == val) {
        node->nodeDepth = deep;
        return node;
    }
    if (val < node->value) {
        deep++;
        return search(node->left, val, deep);
    }
    deep++;
    return search(node->right, val, deep);
}


//重构
void ScapegoatTree::rebuild(Node * scapegoat)
{
    balanceTimes++;
    if(scapegoat == nullptr) {
        return;
    }
    vector<int> key;
    key.push_back(scapegoat->value);
    getKeys(scapegoat->left, &key);
    getKeys(scapegoat->right, &key);
    int size = key.size();
    for(int i = 0; i < size - 1; i++){
        for(int j = i + 1; j < size; j++){
            if(key[i] > key[j]){
                swap(key[i], key[j]);
            }
        }
    }
    if(size % 2 == 0) {
        scapegoat->value = key[size / 2 - 1];
        balance(scapegoat->left, 0, size / 2 -2, key);
        balance(scapegoat->right, size / 2, size - 1, key);
    } else {
        scapegoat->value = key[size / 2];
        balance(scapegoat->left, 0, size / 2 - 1, key);
        balance(scapegoat->right, size / 2 + 1, size - 1, key);
    }

    setSize(scapegoat);
    getParent(root);
}

void ScapegoatTree::setSize(Node *&goal)
{
    if(goal == NULL)
        return;
    int left = getSize(goal->left);
    int right = getSize(goal->right);
    goal->subtreeSize = left + right + 1;
    setSize(goal->left);
    setSize(goal->right);
}


int ScapegoatTree::getSize(Node *goal)
{
    if(goal == NULL)
        return 0;
    else {
        int left = getSize(goal->left);
        int right = getSize(goal->right);
        int final = left + right + 1;
        goal->subtreeSize = final;
        return final;
    }
}


void ScapegoatTree::balance(Node *&goal, int start, int end, vector<int> key)
{
    if(start == end) {
        goal = new Node(key[start]);
        return;
    } else if(end - start == 1) {
        goal = new Node(key[start]);
        balance(goal->right, end, end, key);
        return;
    } else {
        int bt = end - start;
        int mid = bt / 2 + start;
        goal = new Node(key[mid]);
        balance(goal->left, start, mid - 1, key);
        balance(goal->right, mid + 1, end, key);
    }
    return;
}

void ScapegoatTree::getKeys(Node *goal, vector<int> *key)
{
    if(goal != nullptr)
    {
        key->push_back(goal->value);
        getKeys(goal->left,key);
        getKeys(goal->right, key);
        delete  goal;
        goal = NULL;
    }
    return;
}

void ScapegoatTree::getParent(Node * node)
{
    if(node->left != nullptr && node->right != nullptr) {
        node->left->parent = node;
        node->right->parent = node;
        getParent(node->left);
        getParent(node->right);
    } else if(node->left == nullptr && node->right != nullptr){
        node->right->parent = node;
        getParent(node->right);
    } else if(node->right == nullptr && node->left != nullptr) {
        node->left->parent = node;
        getParent(node->left);
    } else {
        return;
    }
}

//从插入到根找到goat
Node * ScapegoatTree::findScapegoat(Node * node, double alpha)
{
    while(node->parent != nullptr) {
//        double alphaCom = (node->subtreeSize * 1.0) / (node->parent->subtreeSize * 1.0);
//        if(alphaCom <= alpha) {
//        double alphaCom = (node->subtreeSize * 1.0) / (node->parent->subtreeSize * 1.0);
//        if((node->subtreeSize * 1.0) <= (alpha * (node->parent->subtreeSize * 1.0))) {
        if(node->parent->left != nullptr && node->parent->right != nullptr) {
            if(max(node->parent->left->subtreeSize, node->parent->right->subtreeSize) <= (alpha * node->parent->subtreeSize)) {
                node = node->parent;
            } else {
                return node->parent;
                break;
            }
        } else {
            if(node->subtreeSize <= (alpha * node->parent->subtreeSize)) {
                node = node->parent;
            } else {
                return node->parent;
                break;
            }
        }
    }
    return node;
}


//have it?
bool ScapegoatTree::contains(int value)
{
    return search(root, value, 0) != nullptr;
}


//α*log(1/α)(size)
unsigned ScapegoatTree::hAlpha(size_t size, double alpha)
{
    return floor(log(size) / log(1 / alpha));
//    unsigned halpha = log(size) / log(1/alpha);
//    return halpha;
}


//删除
void ScapegoatTree::Delete(int key)
{
    // TODO
    Node * node = search(root, key, 0);
    if (node == nullptr) {
        if(isEmpty()) {
            clear();
        }
        if (treeSize <= (ALPHA * treeMaxSize)) {
            rebuild(root);
            treeMaxSize = treeSize;
        }
        return;
    }
    if(isEmpty()) {
        clear();
//        return;
    }
    removeNode(node, root);
    treeSize--;
    if (treeSize <= (ALPHA * treeMaxSize)) {
        rebuild(root);
        treeMaxSize = treeSize;
    }
    return;
}

bool ScapegoatTree::isEmpty()
{
    if(root == nullptr) {
        return true;
    }
    return false;
}

void ScapegoatTree::clear()
{
    treeSize = 0;
    treeMaxSize = 0;
}



//删除节点
void ScapegoatTree::removeNode(Node * node, Node *&root)
{
    if (node->left != nullptr && node->right != nullptr) {
        //找右子树的最小值
        Node * start = node->right;
        while (start->left != nullptr) {
            start = start->left;
        }
        node->value = start->value;
        removeNode(start, root);
        return;
    }
    Node * start = node;
    while (start->parent != nullptr) {
        start->parent->subtreeSize--;
        start = start->parent;
    }
    if (node->subtreeSize == 1 && (node->left == nullptr) && (node->right == nullptr)) {
        retie(node->parent, ParentorRoot(node, root), nullptr);
    } else if (node->left == nullptr) {
        retie(node->parent, ParentorRoot(node, root), node->right);
    } else if (node->right == nullptr) {
        retie(node->parent, ParentorRoot(node, root), node->left);
    }
    node->left = nullptr;
    node->right = nullptr;
    delete node;
}


void ScapegoatTree::levelOrder(Node * root) {
    queue<Node*> q;
    if(root == NULL) {
        return;
    }
    while(!q.empty()) {
        q.pop();
    }
    q.push(root);
    while(!q.empty()){
        int size = q.size();
        for(int i = 0; i < size; i++){
            Node * front = q.front();
            cout << front->value <<" ";
            q.pop();
            if(front->left){
                q.push(front->left);
            }
            if(front->right) {
                q.push(front->right);
            }
        }
        cout << endl;
    }
    cout << endl;
}



//插入节点
Node* ScapegoatTree::insertNode(Node* node, Node * newNode, std::size_t & curDepth)
{
    if (node == nullptr) {
        return newNode;
    }
    if (newNode->value < node->value) {
        curDepth++;
        retie(node, node->left, insertNode(node->left, newNode, curDepth));
    } else {
        curDepth++;
        retie(node, node->right, insertNode(node->right, newNode, curDepth));
    }
    setSize(node);
    return node;
}

//联系
void ScapegoatTree::retie(Node * newParent, Node *& place_to_set, Node * newChild)
{
    if (newChild != nullptr) {
        newChild->parent = newParent;
    }
    place_to_set = newChild;
}


void ScapegoatTree::seeWhat(Node * root){
    cout<< root->value << " ";
    if(root->left!= nullptr)
        seeWhat(root->left);
    if(root->right != nullptr)
        seeWhat(root->right);
}


Node *& ScapegoatTree::ParentorRoot(Node * node, Node *& root)
{
    if (node->parent == nullptr) {
        return root;
    }
    if (node->parent->left != nullptr && node->parent->left->value == node->value) {
        return node->parent->left;
    }
    return node->parent->right;
}




