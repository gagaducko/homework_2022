#include "FixedSP.h"

vector<int> FixedSP::getFixedPointShortestPath(int source, vector<int> intermediates)
{
    vector<int> path;
    // TODO
    middlePoint = intermediates;
    start = source;
    getPath(path);
    //after get path need to clear to make sure next time is ok
    allSort.clear();
    truePath.clear();
    middlePoint.clear();
    haveRoad = true;
    //TODO UPSIDE
    return path;
}

//得到各个两点间的最短距离
void FixedSP::getMinLen()
{
    for(int i = 0; i < size; i++) {
        vector<int> lenMin_middle;
        for(int j = 0; j < size; j++) {
            lenMin_middle.push_back(dijk_len(i, j));
        }
        lenMin.push_back(lenMin_middle);
    }
}


void FixedSP::getPath(vector<int> &path) {
//得到最佳的路径
    allSortit();        //先要得到全排序
    int best = 0;
    int allLen = INF;   //先定一个最大长度
    int sum = allSort.size();
    for(int i = 0; i < sum; i++) {
        //对n个all sort进行一下长度的计算
        int length = 0;
        int j = allSort[i].size();
        for(int k = 0; k < j - 1; k++) {
            int v1 = allSort[i][k];
            int v2 = allSort[i][k + 1];
            length += lenMin[v1][v2];
           // cout<<"the len of "<<v1<< " and " <<v2<<" is "<<dijk_len(v1, v2)<<"  ";
        }
        //cout<< "the len is:" << length<<endl<<endl;
        if(length < allLen) {
            best = i;
            allLen = length;
        }
    }
    if(allLen >= INF) {
        haveRoad = false;
    }
    if(haveRoad == false) {
        vector<int> nullVec = {};
        path = nullVec;
    } else {
        path = allSort[best];
        makeSort(path);
        path = truePath;
    }
}

void swap(vector<int> &a, int i, int j) {
    int temp = a[i];
    a[i] = a[j];
    a[j] = temp;
}

int FixedSP::dijk_len(int v1, int v2) {
    vector<int> best = dijkstra(v1, v2);

    //a little youhua
    vector<int> best2 = dijkstra(v2, v1);
    int len2 = best2.size();
    vector<int> best2T;
    int sum2 = 0;
    for(int i = 0; i < len2 - 1; i++) {
        sum2 += verarc[best2[i]][best2[i + 1]];
    }
    for(int i = len2 - 1; i >= 0; i--) {
        best2T.push_back(best2[i]);
    }

    int sum = 0;
    if(best.empty()) {
        //如果说返回的是一个空的best,那intreturn 回去的就需要是一个很大的数
        return INF;
    }
    int len = best.size();
    for(int i = 0; i < len - 1; i++) {
        sum += verarc[best[i]][best[i + 1]];
    }
    if(sum2 < sum) {
        sum = sum2;
    }
    return sum;
}

int FixedSP::dijk_len_2(int v1, int v2) {
    vector<int> best = dijkstra(v1, v2);
    int sum = 0;
    if(best.empty()) {
        //如果说返回的是一个空的best,那intreturn 回去的就需要是一个很大的数
        return INF;
    }
    int len = best.size();
    for(int i = 0; i < len - 1; i++) {
        sum += verarc[best[i]][best[i + 1]];
    }
    return sum;
}


void FixedSP::all_sort(vector<int> &a, int p, int q)
{
    if(p == q) {
        allSort.push_back(a);
    } else {
        for(int i = p; i < q; i++) {
            swap(a, i, p);
            all_sort(a, p + 1, q);
            swap(a, i, p);
        }
    }
}

void FixedSP::allSortit()
{
    //获得从开始到结束的整条链
    int len = middlePoint.size();
    all_sort(middlePoint, 0, len);
    int sum = allSort.size();
    for(int i = 0; i < sum; i++) {
        allSort[i].push_back(start);
        allSort[i].insert(allSort[i].begin(), start);
    }
}

//从all sort到详细的路径
void FixedSP::makeSort(vector<int> path) {
    int len = path.size();
    for(int i = 0; i < len - 1; i++) {
        int v1 = path[i];
        int v2 = path[i + 1];
        vector<int> tmp = dijkstra(v1, v2);
        vector<int> tmp2 = dijkstra(v2, v1);
        if(dijk_len_2(v1, v2) > dijk_len_2(v2, v1)) {
            int si = tmp2.size();
            vector<int> tmp3 = {};
            for(int m = si - 1; m >= 0; m--) {
                tmp3.push_back(tmp2[m]);
            }
            tmp = tmp3;
        }
        int k = tmp.size();
        for(int j = 0; j < k; j ++) {
            if((i != 0) && (j == 0)) {
                int sizeT = truePath.size();
                if(tmp[j] == truePath[sizeT - 1]) {
                    continue;
                }
            }
            truePath.push_back(tmp[j]);
        }
    }
}

vector<int> FixedSP::dijkstra(int v1, int v2) {
    vector<int> path;
    vector<int> tableLen;         //起点节点v1到每个节点的距离，初始均为INF
    vector<int> tableBefore;      //每个节点的上一个节点，v1节点的上一个节点为-2
    vector<int> bestPoint;        //标记最优节点,即已经访问了的节点
    vector<int> dest;             //用于在最终储存v1,v2的最短路径

    //初始化
    for(int i = 0; i < size; i++) {
        tableLen.push_back(INF);
        tableBefore.push_back(-1);
        bestPoint.push_back(0);
    }
    //节点V1自己到自己，距离一定为0，并且将节点v1的上一个节点设置为-2，并将v1节点设置为最优节点
    tableLen[v1] = 0;
    bestPoint[v1] = 0;
    int bp;
    bool flag = true;

    while(flag) {
        //先要确定最优点
        int min = -5;
        int min_val = INF;
        for(int i = 0; i < size; i++) {
            //每个点都遍历一下
            if((bestPoint[i] == 0) && (tableLen[i] < min_val)) {
                //如果这个点还没有优过并且它的len小于INF的话
                //找其中最小的点
                min_val = tableLen[i];
                min = i;
            }
        }
        if(min == -5) {
            //如果找不到优点，即没有点即没有优过且走到过
            flag = false;
            break;
        }

        bestPoint[min] = 1;
        bp = min;

        //每一次都从最优节点沿路径寻找
        for(int i = 0; i< size; i++) {
            if((tableLen[bp] + verarc[bp][i]) < tableLen[i]) {
                //如果说下一个点的和小于了他的tablelen
                tableLen[i] = tableLen[bp] + verarc[bp][i];
                //重置tablelen并且将这个点的上一个点设置为bp点
                tableBefore[i] = bp;
            }
        }

        //接着就可以看看flag需不需要置false
        flag = false;
        for(int i = 0; i < size; i++) {
            if(bestPoint[i] == 0 && tableBefore[i] >= 0) {
                flag = true;
                break;
            }
        }
    }
    int pathNum = v2;
    while(pathNum >= 0) {
        path.insert(path.begin(), pathNum);
        pathNum = tableBefore[pathNum];
    }
    if(path[0] != v1) {
        vector<int> nullVec = {};
        return nullVec;
    }
    return path;
}
