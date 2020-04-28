//
//  main.cpp
//  Master
//
//  Created by 周华健 on 2019/12/17.
//  Copyright © 2019 周华健. All rights reserved.
//

#include <iostream>
#include <string>
#include <time.h>
#include <sys/time.h>
#include "client.hpp"
#include "manageToMaster.hpp"
#include "webAPI.hpp"
using namespace std;

double get_wall_time_main(){
	struct timeval time;
	if (gettimeofday(&time,NULL)) return 0;
	return (double)time.tv_sec + (double)time.tv_usec * .0000001;
}
int main(int argc, const char * argv[]) {
	int manualSplitQuery = 0;
    const char* re;
	double createstart = get_wall_time_main();
    create();
	double createend = get_wall_time_main();
	cout << "load数据库库耗费的时间:" << createend - createstart << "秒" << endl;
	int i = 1;
    while(1){
		string Dir = "./Query/";
		string query = "";
		string str;
		cout << "是否启用查询预分解代替查询分解器   0：否 ； 1：是" << endl;
		cin >> manualSplitQuery;
		if(manualSplitQuery){
			cout << "系统将使用预先分解好的subQuery/queryForDecompose"<<i<<"查询语句进行计算" << endl;
		}else{
			cout << "输入子查询文件名：" << endl;
			cin >> str;
			Dir += str;
			ifstream in(Dir);
			if (!in) {
				cout << "请重新输入：" << endl;
				continue;
			}
			string line;
			while (getline(in, line)) {
				query += line + '\n';
			}
		}
		double startTime = get_wall_time_main();
		re = queryToWeb((char*)query.c_str(),manualSplitQuery);
		double endTime = get_wall_time_main();
		cout<<"总共耗费的时间:"<<endTime - startTime<<"秒"<<endl;
    }
    closeDb();
    return 0;
}
