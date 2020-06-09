//  提供给上层的接口

#ifndef webAPI_hpp
#define webAPI_hpp

#include "manageToMaster.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string.h>
#include <algorithm>
#include <fstream>
#include "Master.h"
//extern const size_t	STORE_START_NUM;
//extern size_t STORE_COMPUTE_SPLIT;
using namespace std;
#define PORT 10008  //slave服务器端口
extern manageToMaster* manage;

extern "C"{
//查询语句返回
const char *queryToWeb(char *querySen, int manualSplitQuery);

//开启服务器，加载数据库，加载索引信息
bool create();

//关闭服务器，关闭数据库
void closeDb();

//回收内存
void resFree();

void* startSlave(void* ip);//没有被用到过
}
#endif /* webAPI_hpp */
