//
//  masterToSlave.hpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/16.
//  Copyright © 2019 周华健. All rights reserved.
//   Master和slave节点通信

#ifndef masterToSlave_hpp
#define masterToSlave_hpp

#include <stdio.h>
#include "queryClass.hpp"
#include "generalQuery.hpp"
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <list>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include<arpa/inet.h>
#include <unistd.h>
using namespace std;

//默认端口10001
#define PORT_MASTER 10008
#define IP "0.0.0.0"

extern  pthread_rwlock_t rwlock;
//服务器类
class serverSlave{
private:
    list<int> connList;   //tcp链接队列
    unsigned int port;     //端口，默认是10001
    int socketID;    //sock标记
    int createSocket();
    int myBind();
    int mylisten();
public:
    sockaddr_in serverAddr;
    socklen_t len;
    void getConn();
    bool myRec(int conn, void* buffer);
    bool mySend(int conn, void* buffer, size_t size);
    bool getAndSendData();
    bool isClose(int conn);
    serverSlave();
    serverSlave(unsigned int port1);
    serverSlave(serverSlave& temp) = delete;
    serverSlave& operator=(serverSlave& temp) = delete;
    bool mystart();
    bool myclose();
    list<int> getConnList() const;
    bool ereaConnList(int conn);
    bool addConnList(int ids);
    unsigned int getPort() const;
    int getSocketID() const;
    
};

//开始链接，由于pthread不能传递成员函数，故创建此函数
extern void* serverStartConn_Master(void* serv);

#endif /* masterToSlave_hpp */
