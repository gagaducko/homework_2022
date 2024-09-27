#include <vector>
#include <string>
#include <string.h>
#include <iostream>

using namespace std;

#define INF 1e8

class FixedSP
{
private:
    int size;                   //顶点个数
    int start;                  //起点与终点
    bool haveRoad;              //是否存在路径
    vector<vector<int>> verarc;     //matrix
    vector<vector<int>> allSort;    //全排序
    vector<vector<int>> lenMin;     //各个点的最短距离
    vector<int> truePath;   //最终的真正path
    vector<int> middlePoint;        //需要经过的中间节点

public:
    //use to test
    FixedSP(int n, int s, vector<vector<int>> v) {
        size = n;
        verarc = v;
        haveRoad = true;
        start = 93;
        middlePoint = {181, 57, 103, 197, 108, 169, 67, 154};
        getMinLen();
    }

    //need
    FixedSP(vector<vector<int>> matrix) {
        verarc = matrix;
        size = verarc.size();
        haveRoad = true;
        getMinLen();
    }
    ~FixedSP() {}
    void all_sort(vector<int> &a, int p, int q);
    void allSortit();
    vector<int> dijkstra(int v1, int v2);
    void getMinLen();
    int dijk_len(int v1, int v2);
    void getPath(vector<int> &path);
    void makeSort(vector<int> path);
    int dijk_len_2(int v1, int v2);

    vector<int> getFixedPointShortestPath(int source, vector<int> intermediates);
};
