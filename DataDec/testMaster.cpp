/*  testMaster.cpp */
/*
   本文件为测试Master.h头文件函数专用
*/

#include "Master.h"
#include "MasterToWeb.h"
#include "Client.h"
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include <vector>

using namespace std;

int main(int argc, char **argv) {
	vector <string> slaveName;     //slave名字

/*
   本段代码是输入salve节点
*/
	// Created by peng on 2019-12-18, 21:37:54
	// TODO: deployment warning
	slaveName.push_back("node2");
	slaveName.push_back("node4");
	slaveName.push_back("node6");
	
	if (argc != 2) {
		cout << "usage master <num>" << endl;
		return 0;
	}
	if (strcmp(argv[1], "0") == 0) {
		//测试queryToWeb接口
		
		// Created by peng on 2019-12-18, 21:46:52
		// load子节点BitmapBuffer和StatisticBuffer
		create();
		
		// Created by peng on 2019-12-18, 15:34:19
		// load uri and predicate table; load statistic info.
		loadTableAndStatistic("subData");
		
		//char *myQuery = "select ?x ?y ?z where {\n        \"麦格纳\" \"主要产品\" ?x .\n        ?x \"构成\" ?y .\n        ?y \"知名品牌\" ?z .\n}";
		//char *myQuery = "select ?x where { \"播种机\" \"种类\" ?x }";
		//char* myQuery = "SELECT ?x WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent> .}";
		
		string s;
		while (1) {
			std::getline(std::cin, s);
			if (s == "exit") {
				break;
			}
			char qu[100];
			strcpy(qu, s.c_str());
			cout << "qu: " << qu << endl;
			const char *rst = queryToWeb(qu);
			cout << rst << endl;
		}
		
		releaseTableAndStatistic();
		
		closeDb();
		
	} else {
		// 测试能否正确创建数据库专用
		string dataFile;   //数据文件名
		string last;
		cout << "请输入原始数据文件名，不带格式后缀" << endl;
		cin >> dataFile;
		cout << "请输入数据文件格式，如txt，区分大小写，若无后缀输入空格" << endl;
		cin >> last;
		createDatabase(dataFile, slaveName, last, "./subData");
		// Created by peng on 2019-12-18, 15:34:19
		// load uri and predicate table; load statistic info.
		//loadTableAndStatistic("subData");
	}
	return 0;
	
	
	
	/*
 // 本段代码为测试dataDecompose函数专用
   
   vector<string> subDataFile;
   string dataFile;
   string last;

  cout<<"输入原始数据文件名,数据存放在data目录下(不带文件后缀) :"<<endl;
  cin>>dataFile;
  
  cout<<"输入查询文件后缀格式名,区分大小写，如txt,空代表无后缀"<<endl;
  cin>>last;

  subDataFile = dataDecompose(dataFile, last);
  cout<<"输出子数据名，不带后缀格式，默认保存在./subData目录下"<<endl;
  for(int i = 0; i < subDataFile.size(); i++){
    cout<<subDataFile.at(i)<<endl;
  }


*/
	/*
  //本段代码为测试queryDecompose函数专用

  string queryFile;
  string allSubQuery;
  vector<string> subQuery;

  cout<<"输入原始查询语句文件名:"<<endl;   //测试查询语句分解函数,query放在./query里面，结果放在./subQuery里面
  cin>>queryFile;
  allSubQuery = queryDecompose(queryFile);
  cout<<allSubQuery<<endl;
  subQuery = readAllQuery(allSubQuery);
  for(int i = 0; i < subQuery.size(); i++)
   cout<<"查询子文件名:"<<subQuery.at(i)<<endl;
*/


/*
  //本段代码为测试能否正确发送数据文件专用

   vector<string> subDataFile;
   string dataFile;
   string last;

  cout<<"输入原始数据文件名,数据存放在data目录下(不带文件后缀) :"<<endl;
  cin>>dataFile;

  cout<<"输入查询文件后缀格式名,区分大小写，如txt,空代表无后缀"<<endl;
  cin>>last;

  subDataFile = dataDecompose(dataFile, last);
  cout<<"输出子数据名，不带后缀格式，默认保存在./subData目录下"<<endl;
  for(int i = 0; i < subDataFile.size(); i++){
    cout<<subDataFile.at(i)<<endl;
  }
  for(int i = 0; i < subDataFile.size(); i++){
      sendData(subDataFile.at(i), slaveName.at(i), last);
   }
*/


/*
 // 本段代码为测试子查询文件能否发到slave节点专用


  string queryFile;
  string allSubQuery;
  vector<string> subQuery;

  cout<<"输入原始查询语句文件名:"<<endl;   //测试查询语句分解函数,query放在./query里面，结果放在./subQuery里面
  cin>>queryFile;
  allSubQuery = queryDecompose(queryFile);
  cout<<allSubQuery<<endl;
  subQuery = readAllQuery(allSubQuery);
  for(int i = 0; i < subQuery.size(); i++)
   cout<<"查询子文件名:"<<subQuery.at(i)<<endl;
  
  for(int i = 0; i < slaveName.size(); i++){
    for(int j = 0; j < subQuery.size(); j++){
      sendQuery(subQuery.at(j), slaveName.at(i));
    }
  }

*/

/*
  //测试是否能够正确执行语句
  string queryFile;
  string allSubQuery;
  vector<string> subQuery;

  cout<<"输入原始查询语句文件名:"<<endl;   //测试查询语句分解函数,query放在./query里面，结果放在./subQuery里面
  cin>>queryFile;
  allSubQuery = queryDecompose(queryFile);
  cout<<allSubQuery<<endl;
  subQuery = readAllQuery(allSubQuery);
  for(int i = 0; i < subQuery.size(); i++)
   cout<<"查询子文件名:"<<subQuery.at(i)<<endl;

  for(int i = 0; i < slaveName.size(); i++){
    for(int j = 0; j < subQuery.size(); j++){
      sendQuery(subQuery.at(j), slaveName.at(i));
    }
  }

//测试能否执行查询语句
  vector<string> result = executeQuery(subQuery, slaveName);
*/
}
