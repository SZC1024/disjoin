//
//  partitionToQuery.hpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//   查询类中的分区，对应每个节点slave

#ifndef partitionToQuery_hpp
#define partitionToQuery_hpp

#include "subQuery.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string.h>
#include <algorithm>

using namespace std;
//分区编号从1开始，0代表无效分区

//查询计划结构体
struct structPlan{
    size_t ID;  //子查询类ID
    int type;   //0表示查询语句，1表示union，2表示join，3表示分裂
};

class partitionToSub{
private:
    size_t ID;   //分区ID
    unordered_map<size_t, subQuery*> subRef;   //子查询映射
    vector<string> subQueryVec;      //该分区的子查询语句//经过观察，这个参数没有被用到过
    //vector<struct structPlan> subPlan;   //子查询计划
    
public:
    vector<struct structPlan> subPlan;   //子查询计划

    partitionToSub(size_t id, unordered_map<size_t, string> unMap, unordered_map<size_t, size_t>unMapre, unordered_map<size_t, vector<string> > unMap_name);
    
    bool alterSubPlan(vector<structPlan> temp);  //赋值查询计划
    
    bool addSubref(size_t id, subQuery * sub);  //增加子查询
    
    vector<structPlan> getSubPlan() const;     //得到查询计划
    
    size_t getSubPlanSize() const;   //得到查询计划元素个数
    
    subQuery* getSubQuery(size_t id) const;    //根据ID得到某子查询
    
    vector<string> getSubQueryVec() const;    //得到查询语句
    
    size_t getSubQueryVecSize() const;     //得到查询语句个数
    
    vector<size_t> getAllSubID() ;   //得到分区所有子查询ID
    
    vector<string> getSubQueryStr(size_t id) const; //根据ID得到它的子查询
    
    vector<string> getSubQueryName(size_t id) const;  //根据ID得到子查询变量名
    
};


#endif /* partitionToQuery_hpp */
