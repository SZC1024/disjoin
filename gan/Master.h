/* Master.h */

#ifndef MASTER_H
#define MASTER_H

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <fstream>

#include "TripleBit.h"
#include "TripleBitBuilder.h"
#include "TempFile.h"
#include "PredicateTable.h"
#include "URITable.h"
#include "SPARQLLexer.h"
#include "SPARQLParser.h"

#include <string.h>
#include <pthread.h>

using namespace std;

extern char *DATABASE_PATH;
extern PredicateTable *preTable;
extern URITable *uriTable;
// statistics buffer;
extern StatisticsBuffer **statBuffer[SLAVE_NUM];

string intToString(int temp);   //将int转换为string函数

//读取分解api得到的总查询文件，将其分解成多个子查询文件，并返回多个子查询文件的文件名，非路径
vector <string> readAllQuery(string &fileName);

//数据分解文件，dataFile为文件名，last为文件后缀名，返回子数据文件名，不带后缀
vector <string> dataDecompose(string &dataFile, string &last);

//查询语句文件分解，queryFile为查询语句文件名，不带后缀，返回总子查询文件名
string queryDecompose(string &queryFile);

//将数据文件发送到slave节点，dataFile为文件名，desslave为目的slave节点名，last为文件格式后缀
int sendData(const string &dataFile, string &desSlave, const string &last);

//将查询文件发送到slave节点，queryfile为查询文件名，desslave为目的slave节点
int sendQuery(string &queryFile, string &desSlave);

//在slave节点执行单个子查询，queryFile为查询文件名，slaveName为slave节点名，返回结果文件名，无后缀
string executeSingleQueryToOneSlave(string &queryFile, string &slaveName);

//将slave节点的执行结果发送到master节点，queryResult为文件名，slaveName为slave节点名
int sendResultToMaster(string &queryResult, string &slaveName);

//执行所有子查询，返回结果
vector <string> executeQuery(vector <string> &queryFile, vector <string> &slaveName);

//提供给上面的web接口,queryFile为查询文件名,返回结果
vector <string> allOperationOfQuery(string &QueryFile);

//在某个slave节点上面生成数据库，dataFile为文件名， slavename为节点名， last是文件后缀
int createSingleDatabase(const string &dataFile, string &slaveName, const string &last);

//创建分布式数据库总操作，dataFile是数据文件，slaveName是节点名数组， last是文件后缀
int createDatabase(string &dataFile, vector <string> &slaveName, string &last, string targetDir);

int loadTableAndStatistic(const string &dir);

int releaseTableAndStatistic();

//执行spark部分，并返回结果
vector <string> executeSpark(vector <string> &resultFile, vector <string> &slaveName);

//Master节点开启slave节点上的服务器监听
void startServer(vector <string> slaveName);

void realDataDecompose(TempFile &rawFacts, TempFile **slavesTempFile);

void formatTempFile(TempFile &rawFacts, const string &targetDir);

string transformQuery(string query);

string generateTransformedQuery(SPARQLParser *parser);

ID getIDByUri(const string uri);

string getUriByID(ID id);

ID getIDByPredicate(const string pre);

string getPredicateByID(ID pid);

size_t getResultSize(string query, size_t slaveID);

#endif
/* Master.h */
