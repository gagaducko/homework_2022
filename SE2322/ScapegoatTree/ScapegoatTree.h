#include <vector>
#include <iostream>
#include <cmath>
#include <queue>
using namespace std;

#define ALPHA 0.6

struct Node {
    int value;              //权值
    Node *left = nullptr;
    Node *right = nullptr;
    Node *parent = nullptr;     //左右子节点与父节点
    size_t subtreeSize;
    int nodeDepth = 0;
    Node() = default;
    Node(int val) : value(val){
        subtreeSize = 1;
    }
};

class ScapegoatTree
{
public:
    Node *root = nullptr;
    size_t treeSize = 0;
    size_t treeMaxSize = 0;
    int balanceTimes = 0;


    ScapegoatTree() { }
    ~ScapegoatTree() {
        delete root;
    }

    void levelOrder(Node * root);
    Node* insertNode(Node* node, Node * newNode, std::size_t & curDepth);
    void retie(Node * new_parent, Node *& place_to_set, Node * new_child);
    void removeNode(Node * node, Node *&root);
    Node *& ParentorRoot(Node * node, Node *& root);
    unsigned hAlpha(size_t size, double alpha);
    bool contains(int value);
    Node * findScapegoat(Node * node, double alpha);
    void rebuild(Node * scapegoat);
    Node * flattenTree(Node * node, Node * head);
    Node * search(Node * node, int val, int deep);
    void seeTree(Node * node);
    void seeWhat(Node *root);
    void getKeys(Node *goal, vector<int> *key);
    void balance(Node *&goal, int start, int end, vector<int> keys);
    void setSize(Node *&goal);
    int getSize(Node *goal);
    void getParent(Node * node);
    bool isEmpty();
    size_t getHeightNode(Node * node, int key, size_t & deep);
    Node * searchHave(Node * node, int val, size_t & deep);
    void clear();
    /**
     * @brief Get the Rebalance Times
     *
     * @return int
     */
    int GetRebalanceTimes();

    /**
     * @brief Look up a key in scapegoat tree. Same as trivial binary search tree
     *
     * @param key
     */
    void Search(int key);

    /**
     * @brief Insert a new node into the tree. If the key is already in the tree, then do nothing
     *
     * @param key
     */
    void Insert(int key);

    /**
     * @brief Delete a node of the tree. If the key is not in the tree, do nothing.
     *
     * @param key
     */
    void Delete(int key);
};
