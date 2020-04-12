//
//  generalQuery.hpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/15.
//  Copyright © 2019 周华健. All rights reserved.
//

#ifndef generalQuery_hpp
#define generalQuery_hpp

#include "queryClass.hpp"
#include "comClass.hpp"
#include "comClassClient.hpp"
#include "common.h"
#include <stdio.h>
#include<iostream>
#include<stdlib.h>
#include<vector>
#include<unordered_map>
#include<unordered_set>
#include<algorithm>
#include<string.h>
#include<fstream>
static const size_t STORE_START_NUM = 100000;//在host文件中，如果是存储与计算分离架构的话，存储节点的开始编号是100001，开始递增
static size_t STORE_COMPUTE_SPLIT;//当存储与计算分离架构时，该值为1
using namespace std;

class generalQuery{
public:
	unordered_map<size_t, string> subQueryStr;  //该节点所拥有的的子查询语句及ID映射
	unordered_map<size_t, vector<string> > subQueryNamevec; //ID和查询变量名映射
    vector<struct structPlan> plan;  //根节点为最终结果，从0开始
private:
    size_t ID;    //查询类ID
    size_t myNodeId;
    unordered_map<size_t, size_t> globalSubQueryID;  //全局子查询ID,及初始产生节点编号
    unordered_map<size_t, queryClass* > ownedSubQuery; //拥有的子查询结果
    //unordered_map<size_t, string> subQueryStr;  //该节点所拥有的的子查询语句及ID映射
    //unordered_map<size_t, vector<string> > subQueryNamevec; //ID和查询变量名映射
    unordered_map<size_t, client*> unmap_cl;      //对应某slave节点的客户端
    unordered_map<size_t, string> serverIPRef;    //节点IP映射，除自身节点

public:
    generalQuery();
    generalQuery(size_t id, unordered_map<size_t, string> sub, unordered_map<size_t, vector<string> > nameUmap);
    generalQuery(generalQuery& query);
    generalQuery& operator= (generalQuery& query);
    bool alterSubQueryID(unordered_map<size_t, size_t> umap);  //变更全局子查询ID映射
    size_t addSubQuery(size_t id, queryClass* query);  //增加拥有的结果
    size_t getID() const;         //得到查询ID
    size_t getNode(size_t id) const;   //根据自查询ID得到它产生于哪个节点
    queryClass* getSubQueryClass(size_t id); //根据ID得到子查询
    vector<string> getName(size_t id) const;  //得到ID到变量名的映射
    bool alterPlan(vector<structPlan> plan1);   //变更查询计划
    bool executePlan();   //执行查询计划
    bool recycleEx(size_t index);   //循环执行查询树
    bool executeSubQuery();
};

#endif
/* generalQuery_hpp */
