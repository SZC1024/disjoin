//
//  comClass.hpp
//  garphDatabase
//
//  Created by songzc on 2019/12/15.
//  Copyright © 2019 songzc. All rights reserved.
//slave节点的客户端和服务器端，slave对slave
#ifndef comClass_hpp
#define comClass_hpp

#include "queryClass.hpp"
#include "generalQuery.hpp"
#include<sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <stdio.h>
#include <iostream>
#include <algorithm>
#include <vector>
#include <list>
#include <string.h>
#include <unistd.h>
#include<arpa/inet.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/ioctl.h>

//默认端口
#define PORT_SLAVE 10001
#define IP "0.0.0.0"

extern pthread_rwlock_t rwlock_slave;

class server{
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
    server();
    server(unsigned int port1);
    server(server& temp);
    server& operator=(server& temp);
    void getConn();
    bool myRec(int conn, void* buffer);
    bool mySend(int conn, void* buffer, size_t size);
    bool getAndSendData();
    bool isClose(int conn);
    bool mystart();  //开启监听
    bool myclose();
    list<int> getConnList() const;
    bool ereaConnList(int conn);
    unsigned int getPort() const;
    bool addConnList(int sid);
    int getSocketID() const;
    
};

//开始链接，由于pthread不能传递成员函数，故创建此函数
extern void* serverStartConn_Slave(void* serv);

#endif /* comClass_hpp */
