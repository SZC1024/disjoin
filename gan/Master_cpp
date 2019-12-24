/* MasterToWeb.cpp */
/*
  MasterToWeb.h的实现文件
  为上层web应用提供动态库函数
*/

#include "MasterToWeb.h"
#include "Master.h"
#include "Client.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <stdlib.h>
#include <string.h>
#include <vector>

using namespace std;
//#define SLAVE_NUM 3
#define MAX_LEN 102400000 //查询语句返回结果的最大字符数

char *resultEnd;

Client *client;

long Port = 10000;

char IP[SLAVE_NUM][20] = {"11.11.11.2", "11.11.11.6", "11.11.11.4"};

static int queryCount = 0; //查询语句文件计数

vector <string> queryComposeToVec(char *querySen, string &subQueryFile) {
	
	vector <string> resultVec;    //结果数组
	string queryFile = "queryC"; //查询语句名，默认queryC
	string Dir = "./Query/";
	queryCount = (queryCount + 1) % 32768;
	string temp = intToString(queryCount);
	queryFile += temp;
	
	temp = Dir + queryFile;     //将查询语句string保存成查询文件
	ofstream out(temp.c_str()); //创建写入文件流out
	out << querySen << endl;    //将查询语句输入
	out.close();
	
	string queryName = queryDecompose(queryFile); //查询文件的分解
	subQueryFile = queryFile;
	resultVec = readAllQuery(queryName); //得到子查询语句数组
	return resultVec;
}

/*
  结果集封装成列
  如：
  A
  B
  c
  合并成A B C
  //其中buffer为某个子查询的结果集，row为列数
*/

vector <string> unionToRow(char **buffer, size_t row) {
	
	cerr << "unionToRow start" << endl;
	string tempStr;
	char *tempChar = (char *) malloc(MAX_LEN);
	set <string> resultSet; //结果集合
	vector <string> result;
	size_t k = 0;
	size_t count = 0; //每row个string组合成一个string，count用于计数a
	
	//断电
	cout << "row: " << row << endl;
	for (size_t i = 0; i < SLAVE_NUM; i++) {
		string str1(buffer[i]);
		cout << "----------str1-" << endl;
		if (str1 == "-1\nEmpty Result\n" || str1 == "Empty Result\n") {
			resultSet.insert("-1\tEmpty Result");
			continue;
		}
		for (size_t j = 0; buffer[i][j] != '\0'; j++) {
			// cout<<buffer[i][j];
			if (buffer[i][j] == '\n') {
				// cout << "inner if" << endl;
				count++;
				if (count == row) {
					count = 0;
					tempChar[k] = '\0';
					string str(tempChar, k);
					resultSet.insert(str);
					k = 0;
					memset(tempChar, 0, MAX_LEN);
				} else {
					count++;
					tempChar[k++] = '\t';
				}
			} else {
				tempChar[k++] = buffer[i][j];
			}
		}
	}
	// cout << "resultSet.size()-" << resultSet.size() << endl;
	set<string>::iterator it;
	for (it = resultSet.begin(); it != resultSet.end(); it++) {
		result.push_back(*it);
		// cout << "*it-" << *it << endl;
	}
	// for(int i = 0; i < result.size(); ++i){
	//	cout << result[i] << endl;
	// }
	free(tempChar);
	return result;
}

/*
  输出一行有几个string
*/

size_t lineStringNum(string temp) {
	
	size_t i;
	size_t count = 0;
	cout << "temp: " << temp << endl;
	for (i = 0; i < temp.size(); i++) {
		if (temp.at(i) == ' ')
			count++;
	}
	if (temp.at(temp.size() - 1) != ' ')
		count++;
	
	return count;
}

/*
 打包成动态库，提供给上层web进行调用
 querySen是查询语句string
 返回结果string
*/

const char *queryToWeb(char *querySen) {
	
	cout << querySen << endl;
	
	vector <string> queryVec; //子查询语句数组
	char *result;
	
	vector <vector<string>> resultAllVec; //所有子查询的结果集
	
	if (client == NULL) {
		cout << "数据库尚未加载，请先加载数据库" << endl;
		return NULL;
	}
	
	string subQueryFile = ""; //子查询文件名
	string Dir = "./subQuery/";
	queryVec = queryComposeToVec(querySen, subQueryFile); //子查询语句数组
	
	cout << "创建读写缓冲区前" << endl;
	char *buffer[SLAVE_NUM]; //创建一个读写缓冲区
	for (int i = 0; i < SLAVE_NUM; i++) {
		buffer[i] = (char *) malloc(MAX_LEN);
	}
	cout << "创建读写缓冲区后" << endl;
	
	//创建一个读文件对象
	string sub = Dir + subQueryFile;
	ifstream in(sub.c_str());
	if (!in.is_open()) {
		cout << "MasterToWeb中queryToWEB函数打开文件失败" << endl;
		return ""; // FIXME
	}
	string temp = "";
	getline(in, temp);
	
	//查询结果集生成
	for (size_t k = 0; k < queryVec.size(); k++) {
		
		for (int i = 0; i < SLAVE_NUM; ++i) {
			memset(buffer[i], 0, MAX_LEN);
		}
		
		getline(in, temp);
		vector <string> tempVec; //临时单个子查询的结果集
		client->mySend(queryVec.at(k).c_str(),
		               queryVec.at(k).size()); //发送查询语句
		cout << "before recv" << endl;
		client->myRecv(buffer, MAX_LEN); //收到结果
		// for(int i = 0; i < SLAVE_NUM; ++i){
		//	cout << buffer[i] << endl;
		// }
		cout << "after recv" << endl;
		size_t splitnum = lineStringNum(temp);
		cout << "after splitnum" << endl;
		tempVec = unionToRow(buffer, splitnum);
		cout << "after unionToRow" << endl;
		resultAllVec.push_back(tempVec);
		cout << "after pushback" << endl;
	}
	in.close();
	
	//输出结果集
	// for(int i = 0; i < resultAllVec.size(); i++){
	//   for(int j = 0; j < resultAllVec[i].size(); j++)
	//     cout<<resultAllVec[i].at(j)<<endl;
	//  }
	
	//给高的格式
	
	string strToGao = "";
	size_t countstr = 0;
	for (size_t i = 0; i < resultAllVec.size(); i++) {
		for (size_t j = 0; j < resultAllVec[i].size(); j++) {
			if (resultAllVec[i].at(j) == "-1\tEmpty Result")
				continue;
			for (size_t k = 0; k < resultAllVec[i][j].size(); k++) {
				if (resultAllVec[i][j].at(k) == '\t')
					resultAllVec[i][j].at(k) = '|';
			}
			strToGao = strToGao + resultAllVec[i].at(j) + '|';
		}
	}
	
	if (strToGao == "")
		strToGao = "-1|Empty Result";
	else {
		for (size_t i = 0; i < strToGao.size(); i++)
			if (strToGao.at(i) == '|')
				countstr++;
		string str1 = intToString(countstr);
		strToGao = str1 + '|' + strToGao;
		size_t i = strToGao.size();
		if (i != 0 && strToGao.at(i - 1) == '|')
			strToGao.at(i - 1) = '\0';
	}
	
	// cout<<"给高的结果:"<<strToGao<<endl;
	
	result = (char *) malloc(strToGao.size() + 1);
	
	strcpy(result, strToGao.c_str());
	
	for (int i = 0; i < SLAVE_NUM; i++)
		free(buffer[i]);
	
	cout << "给高的结果2:" << result << endl;
	
	//全局变量resultEnd赋值
	resultEnd = result;
	return ((const char *) result);
}

/*
回收内存
*/
void resFree() {
	if (resultEnd != NULL)
		free(resultEnd);
	resultEnd = NULL;
}

/*
  加载数据库函数
*/
int create() {
	
	cout << "正在创建数据库" << endl;
	if (client != NULL) {
		cout << "数据库已经加载，请勿重复加载" << endl;
		return -1;
	}
	
	// Created by peng on 2019-12-18, 11:09:03
	// 此处需要把映射表和统计索引加载到内存中
	
	
	// slave节点开启监听
	// vector<string> slaveName;
	// slaveName.push_back("slave1");
	// slaveName.push_back("slave2");
	// slaveName.push_back("slave3");
	// startServer(slaveName);
	
	/*该段赋值server地址和端口信息*/
	
	struct sockaddr_in serverAddr[SLAVE_NUM];
	
	for (int i = 0; i < SLAVE_NUM; i++) {
		serverAddr[i].sin_family = AF_INET;
		serverAddr[i].sin_port = htons(Port);
		serverAddr[i].sin_addr.s_addr = inet_addr(IP[i]);
	}
	
	client = new Client(serverAddr); //创建clien对象
	client->myConnect();             //连接
	char buffer[] = "create\n";
	client->mySend(buffer, 8); //发送create字符串，通知slave节点
	cout << "已发送create命令" << endl;
	char *recStr[SLAVE_NUM];
	for (int i = 0; i < SLAVE_NUM; i++)
		recStr[i] = (char *) malloc(MAX_LEN);
	cout << "准备接受create ok" << endl;
	client->myRecv(recStr, MAX_LEN); //接受返回的数据
	cout << "create ok返回" << endl;
	for (int i = 0; i < SLAVE_NUM; i++)
		if (strcmp(recStr[i], "create ok\n") != 0) {
			cout << "节点: " << i << "不ok" << endl;
			return -1;
		}
	for (int i = 0; i < SLAVE_NUM; i++)
		free(recStr[i]);
	cout << "最终返回" << endl;
	return 0;
}

/*
  关闭数据库函数
*/

void closeDb() {
	cout << "调用close函数" << endl;
	if (client == NULL)
		return;
	else {
		client->mySend("exit\n", 6);
		client->myClose();
		delete client;
		client = NULL;
	}
}
/* MasterToWeb.cpp */
