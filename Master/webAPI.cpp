//
//  webAPI.cpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//

#include "webAPI.hpp"

manageToMaster* manage;
char* reChar;
const char *queryToWeb(char *querySen){
    const char *result;
    vector<vector<size_t> > reVec;
    vector<vector<string> > strVec;
    string str(querySen);
    size_t id = manage->addQuery(str);
    reVec = manage->exeuteQuery(id);
    //将id转换成字符串
    for(size_t i = 0; i < reVec.size(); i++){
	vector<string> temp;
      for(size_t j = 0; j < reVec[i].size(); j++){
 	 string str;
	 str = getUriByID(reVec[i].at(j));
	 temp.push_back(str);
	}
 	strVec.push_back(temp);
    }
    //将结果转换成gao的格式
    string strRes;
    size_t count = strVec.size();
    strRes = to_string(count) + "|";
    for(size_t i = 0; i < strVec.size(); i++){
	for(size_t j = 0; j < strVec[i].size(); j++){
	   strRes += strVec[i].at(j);
 	   strRes += "|";
	}
    }
    
    //将string存放在堆上
    reChar = (char *) malloc(strRes.size() + 1);
    strcpy(reChar, strRes.c_str());
    result = reChar;
    return result;
}

//加载数据库，开启服务器节点
bool create(){
    
    if(manage == nullptr){
        manage = new manageToMaster();
    }
    else{
        delete manage;
        manage = new manageToMaster();
    }
    
    //读取节点信息
    map<size_t, string> slaveName;//参数1：slave节点编号，参数2：slave的ip地址
    ifstream in("./host");
	if (!in) {
		cout << "配置文件打开失败额" << endl;
		exit(0);
	}
    size_t id;
    in>>id;
    size_t id2;
    string ip;
    while(in>>id2>>ip){
        if (id2 > STORE_START_NUM) STORE_COMPUTE_SPLIT = 1;
        if (id2 == id) continue;
        else{
            slaveName[id2] = ip;
        }
    }
    in.close();
    
    //多线程开启slave节点
    /*for(size_t i = 0; i < slaveName.size(); i++){
        pthread_t ths;
        pthread_create(&ths, nullptr, startSlave, (void*) slaveName.at(i).c_str());
        pthread_detach(ths);
    }
    sleep(5);*/
    map<size_t, client*> slaveclient;
    if (STORE_COMPUTE_SPLIT) {
        for (auto sN : slaveName){
            cout << "slave_" << sN.first << "的ip:" << sN.second << endl;
			if (sN.first > STORE_START_NUM) {
				client* cl = new client(sN.second, PORT);
                slaveclient[sN.first] = cl;
				cl->createSocket();
				if (!cl->myConnect()) continue;
				cl->mySend((void*)"create", 7);
				// char temp[1024];
				//memset(temp, 0, 1024);
				//cl->myRec(temp);
				// string str(temp);
				// cout<<slaveName.at(i)<<" "<<str<<endl;
			}
        }
        for (auto sC : slaveclient) {
            size_t creatok = 0;
            sC.second->myRec(&creatok);
            if (creatok) cout << sC.first << "号slave加载数据库成功" << endl;
            else cout << sC.first << "号slave加载数据库失败！" << endl;
            sC.second->myclose();
        }
    }else{
        for (auto sN : slaveName){
            cout << "slave_" << sN.first << "的ip:" << sN.second << endl;
			client* cl = new client(sN.second, PORT);
            slaveclient[sN.first] = cl;
			cl->createSocket();
			if (!cl->myConnect()) continue;
			cl->mySend((void*)"create", 7);
        }
		for (auto sC : slaveclient) {
			size_t creatok = 0;
			sC.second->myRec(&creatok);
			if (creatok) cout << sC.first << "号slave加载数据库成功" << endl;
			else cout << sC.first << "号slave加载数据库失败！" << endl;
			sC.second->myclose();
		}
    }

    cout << "load table and statistic" << endl;
    loadTableAndStatistic("./subData");
    return true;
}


//关闭数据库
void closeDb(){
    map<size_t, string> slaveName;//参数1：slave节点编号，参数2：slave的ip地址
    ifstream in("./host");
    if(! in){
        cout<<"配置文件打开失败"<<endl;
        exit(0);
    }
    size_t id;
    in>>id;
    size_t id2;
    string ip;
    while(in>>id2>>ip){
        if (id2 > STORE_START_NUM) STORE_COMPUTE_SPLIT = 1;
        if (id2 == id) continue;
        else{
            slaveName[id2] = ip;
        }
    }
    in.close();
    
	if (STORE_COMPUTE_SPLIT) {
		for (auto sN : slaveName) {
			if (sN.first > STORE_START_NUM) {
				client* cl = new client(sN.second, PORT);
				cl->createSocket();
				cl->myConnect();
				cl->mySend((void*)"close", 6);
				cl->myclose();
                cout << "存储节点slave_" << sN.first << "关闭" << endl;
			}
		}
	}else {
		for (auto sN : slaveName) {
			client* cl = new client(sN.second, PORT);
			cl->createSocket();
			cl->myConnect();
			cl->mySend((void*)"close", 6);
			cl->myclose();
            cout << "存储&计算节点slave_" << sN.first << "关闭" << endl;
		}
	}
}

//开始一个节点//没有被用到过
void* startSlave(void* ip){
    string str((char*) ip);
    string Dir = "/root/grace-slave/bin/lrelease/startslave";  //执行文件目录
    string cmdString = "ssh ";
    cmdString += str;
    cmdString += " ";
    cmdString += "\"";
    cmdString += Dir;
    cmdString += "\"";
    
    int pid = system(cmdString.c_str());
    if(pid == 0){
        cout<<str<<"开启监听成功"<<endl;
    }
    else{
        cout<<str<<"开启监听失败"<<endl;
    }
    return nullptr;
}

void resFree(){
    free(reChar);
}
