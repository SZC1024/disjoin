#ifndef manageToSlave_hpp
#define manageToSlave_hpp

# include "comClass.hpp"
# include "masterToSlave.hpp"
# include "queryClass.hpp"
# include "generalQuery.hpp"
# include <stdio.h>
# include <iostream>
# include <algorithm>
# include <stdlib.h>
# include <string.h>
# include <vector>
#include <cassert>
# include <unordered_map>
# include <list>
# include <pthread.h>
#include <unistd.h>
#include <fstream>
#include "../TripleBit/tripleBitQueryAPI.h"
//extern const size_t STORE_START_NUM;
//extern size_t STORE_COMPUTE_SPLIT;
using namespace std;

//slave节点之间服务端默认端口是10001
//slave节点默认与MAster节点通信端口是10008
class manageToSlave{
private:
    size_t ID; //本机编号
    size_t networkTraffic;//表示该节点所有发送出去的数据量（slave to slave），仅统计了重要数据，元数据没有统计
    server* serverToSlave;  //对slave节点之间通信的服务器
    serverSlave* serverToMaster;  //slave和MAster节点的服务器
    unordered_map<size_t, generalQuery*> umap_Gen_Query; //查询类映射
    unordered_map<size_t, string> ipref;   //全局slave节点ip映射
    
public:
    manageToSlave();
    manageToSlave(size_t id);   // 创建的时候创建服务器变量
    manageToSlave(const manageToSlave& temp);
    manageToSlave& operator=(const manageToSlave& temp);
    bool addQuery(size_t id, unordered_map<size_t, string> sub, unordered_map<size_t, vector<string> > nameUmap);//加入一个子查询
    void myStartToSlave();  //开始进行监听和收发数据
    void myStartToMaster();  //开始监听和收发MAster的数据
    bool myclose();      //关闭服务器
    bool alterIPref(unordered_map<size_t, string> ip); //变更ip映射
    bool addIPref(size_t num, string ip);  //增加节点
    unordered_map<size_t, string> getIPref() const;  //得到IP映射
    size_t getID() const;  //返回本机编号
    bool threadSendDataToSlave(int it, size_t* id);//多线程发送
    bool getAndSendData_To_Slave();   //slave对slave节点之间的收发数据
    bool getAndSendData_To_Master();  //slave对master节点服务器的收发数据
    bool removeQuery(size_t id);  //根据ID删除一个查询
    bool is_Empty_Query() const;   //判断是否有查询语句
    bool is_NULL_ServerToSlave() const;  //判断slave对slave服务器是否开启
    bool is_NULL_ServerToMaster() const;    //判断slave对maste节点是否开启
    queryClass* getSubQuery(size_t id1, size_t id2); //得到子查询，id1是总查询id，id2是子查询id
    ~manageToSlave();
};
//由于pthread不能传递成员函数，故创建此函数
//对slave服务器开始工作
extern void* serverStart_Slave(void* ser);

//对master的服务器开始工作
extern void* serverStart_Master(void* ser);

//开始收发数据Slave
extern void* serverGetData_Slave(void* serv);

//开始收发数据,对Master
extern void* serverGetData_Master(void* serv);

#endif /* manageToSlave_hpp */
