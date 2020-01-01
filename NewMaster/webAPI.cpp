//
//  webAPI.cpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//

#include "webAPI.hpp"

manageToMaster* manage;

const char *queryToWeb(char *querySen){
    const char *result;
    vector<vector<size_t> > reVec;
    string str(querySen);
    size_t id =  manage->addQuery(str);
    reVec = manage->exeuteQuery(id);
    
    cout<<"原始数据结果前十条:"<<endl;
    for(size_t i = 0; i < reVec.size(); i++){
        
        if(i > 10) break;
        for(size_t j = 0; j < reVec[i].size(); j++){
            cout<<reVec[i].at(j)<<"\t"<<endl;
        }
        cout<<"下一条"<<endl;
    }
   cout<<"总结果条数："<< reVec.size() << endl;
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
    vector<string> slaveName;
    ifstream in("./host");
    size_t id;
    in>>id;
    size_t id2;
    string ip;
    if(! in){
        cout<<"配置文件打开失败额"<<endl;
        exit(0);
    }
    while(in>>id2>>ip){
        if(id2 == id){
            continue;
        }
        else{
            slaveName.push_back(ip);
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
    for(size_t i = 0; i < slaveName.size(); i++){
        cout<<"ip: "<<slaveName.at(i)<<endl;
        client* cl = new client(slaveName.at(i), PORT);
        cl->createSocket();
        if(!cl->myConnect()) continue;
            cl->mySend((void*)"create", 7);
        // char temp[1024];
        //memset(temp, 0, 1024);
        // cl->myRec(temp);
        // string str(temp);
        // cout<<slaveName.at(i)<<" "<<str<<endl;
        cl->myclose();
    }
    cout << "load table and statistic" << endl;
    //loadTableAndStatistic("./subData");
    return true;
}


//关闭数据库
void closeDb(){
    vector<string> slaveName;
    ifstream in("./host");
    size_t id;
    in>>id;
    size_t id2;
    string ip;
    if(! in){
        cout<<"配置文件打开失败"<<endl;
        exit(0);
    }
    while(in>>id2>>ip){
        if(id2 == id){
            continue;
        }
        else{
            slaveName.push_back(ip);
        }
    }
    in.close();
    
    for(size_t i = 0; i < slaveName.size(); i++){
        client* cl = new client(slaveName.at(i), PORT);
        cl->createSocket();
        cl->myConnect();
        cl->mySend((void*)"close", 6);
        cl->myclose();
        cout<<slaveName.at(i)<<"关闭"<<endl;
    }
    delete manage;
}

//开始一个节点
void* startSlave(void* ip){
    string str((char*) ip);
    string Dir = "/opt/Grace/bin/lrelease/startSlave";  //执行文件目录
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
    
}
