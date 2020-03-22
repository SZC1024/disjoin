#ifndef generalQuery_hpp
#define generalQuery_hpp

#include "subQuery.hpp"
#include "partitionToQuery.hpp"
#include "client.hpp"
#include "planTree.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string.h>
#include <algorithm>
#include <fstream>
#include<unordered_set>
#include <set>
// #include "../gan/Master.h"
#define PORT 10008
using namespace std;

//查询编号从1开始，0代表无效查询
class PlanTree;
class generalQuery{
private:
    size_t ID;    //查询ID
    unordered_map<size_t, partitionToSub* > partSub; //分区映射
    unordered_map<size_t, size_t> globalIDRef; //全局ID映射表,前者代表ID，后者代表节点
    vector<structPlan> plan;   //查询计划树，根节点编号0
    string queryStr;      //查询语句
    vector<string> subStr; //子查询语句
    vector<vector<string>> subStrValName; //子查询语句变量名，和substr一一对应
    unordered_map<size_t, client* > clRef;  //客户端映射
    unordered_map<size_t,  string> ipRef; //子节点IP映射
    vector<vector<size_t> > result;
    size_t MaxSubID;
    vector<string> finalResultName;   //最终的结果名
    
public:
    generalQuery();
    generalQuery(size_t id, string str);  //生成查询类
    bool decomposeQueryAll();  //查询语句的分解
    bool createParition();   //创建paritition
    bool createPlan();     //创建查询计划
    bool decomposePlan(PlanTree* generalPlanTree, size_t num);   //分解查询计划（被createPlan在结尾调用了）
    bool sendPlan();        //发送查询计划
    bool createGlobalRef();   //创建全局映射
    bool sendGlobalRef();    //发送全局映射
    bool sendSubqueryToSlave();   //发送子查询语句和ID
    bool mystart();     //开始执行语句
    bool readAllQuery(string& fileName);  //从文件中读取,赋值查询语句和变量名数组
    string queryDecomposeFile(string& queryFile);  //分解查询文件
    //将查询语句分解成子查询数组，赋值查询变量名数组
    bool queryComposeToVec(const char* querySen);
    bool waitResult();
    vector<vector<size_t> > getResult() const;
    
};




#endif /* generalQuery_hpp */
