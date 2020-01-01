//
//  common.h
//  garphDatabase
//
//  Created by 周华健 on 2019/12/20.
//  Copyright © 2019 周华健. All rights reserved.
//

#ifndef common_h
#define common_h
#include <stdio.h>
#include <vector>
using namespace std;

struct structPlan{
    size_t ID;
    int type;
};

template<typename T>
struct VectorHash {
    size_t operator()(const std::vector<T>& v) const {
        std::hash<T> hasher;
        size_t seed = 0;
        for (auto& i : v) {
            seed ^= hasher(i) + 0x9e3779b9 + (seed<<6) + (seed>>2);
        }
        return seed;
    }
};

//进行vector排序
struct VectorCompare{
    vector<size_t> name;  //列号
    vector<size_t> val;   //数据
};
#endif /* common_h */
