//  全局query

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
#include "Master.h"
#include <time.h>
#include <sys/time.h>
#define PORT 10008
static const size_t STORE_START_NUM = 100000;//在host文件中，如果是存储与计算分离架构的话，存储节点的开始编号是100001，开始递增
static size_t STORE_COMPUTE_SPLIT;//当存储与计算分离架构时，该值为1，否则为0
using namespace std;

//查询编号从1开始，0代表无效查询
class PlanTree;
class generalQuery{
private:
    int manualSplitQuery = 0;//代表查询预分解
    size_t ID;    //查询ID
    unordered_map<size_t, partitionToSub* > partSub; //分区映射（从1开始，+1递增）
    unordered_map<size_t, size_t> globalIDRef; //全局ID映射表,前者代表ID，后者代表节点
    vector<structPlan> plan;   //查询计划树，根节点编号0（暂时没有被用到过）
    string queryStr;      //查询语句
    vector<string> subStr; //子查询语句
    vector<vector<string>> subStrValName; //子查询语句变量名，和substr一一对应
    unordered_map<size_t, client*> clRef;  //客户端映射
    unordered_map<size_t, string> ipRef; //子节点IP映射
    //map<size_t, client*> storeClRef;//存储节点客户端映射，如果host文件（-1标注）中指定有的话
    //map<size_t, string> storeIpRef;//存储节点IP映射，如果host文件（-1标注）中指定有的话
    //store的使用：当本系统作为存储与计算分离架构时，有专门的存储节点用来运行数据库，然后其他普通节点为计算连接的节点，这个时候storeClRef和storeIpRef有用
    //如果为存储计算不分离架构，只有clRef和ipRef有用，所有节点即为存储节点，也是计算节点
    //需要注意的是，当存储与计算分离时，任务下发时所使用的节点统计索引将失效，因为计算节点依赖的所有数据都将由存储节点通过网络传输过来，此时判断节点生成数据量无意义
    vector<vector<size_t> > result;
    size_t MaxSubID;
    vector<string> finalResultName;   //最终的结果名
    map<size_t, subQuery*> idtosubq;  //第一个参数为ID，所有子查询id映射
    
public:
    generalQuery();
    generalQuery(size_t id, string str, int manualSplitQuery);  //生成查询类
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
    bool orderSlaveExecuteSubQuery();//命令slave节点开始执行子查询
    bool orderSlaveExecutePlan();//命令slave节点开始执行查询计划
    vector<vector<size_t> > getResult() const;
    
};




#endif /* generalQuery_hpp */
