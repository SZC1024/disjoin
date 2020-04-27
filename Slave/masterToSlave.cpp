//
//  masterToSlave.cpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/16.
//  Copyright © 2019 周华健. All rights reserved.
//  Master和slave节点通信

#include "masterToSlave.hpp"

#include <stdio.h>

pthread_rwlock_t rwlock;

serverSlave::serverSlave(){
    
    port = PORT_MASTER;
    pthread_rwlock_init(&rwlock, nullptr); //初始化读写锁
}

serverSlave::serverSlave(unsigned int port1){
     
    port = port1;
}

//创建初始套接字
int serverSlave::createSocket(){
    
    socketID = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = inet_addr(IP);
    
    cout<<"对master创建套接字成功"<<socketID<<endl;
    return 1;
}

//绑定端口
int serverSlave::myBind(){
    
 auto temp = ::bind(socketID, (struct sockaddr*) &serverAddr, sizeof(serverAddr));
    if(temp == -1){
        cout<<"对master服务器绑定端口失败"<<endl;
        exit(0);
    }
    else{
        cout<<"对master服务器绑定端口成功"<<endl;
    }
    return temp;
}

//监听端口
int serverSlave::mylisten(){
    
    int temp;
    len = sizeof(serverAddr);
    temp = listen(socketID, 1024); //最多监听1024个队列
    if(temp == -1){
        cout<<"服务器监听失败"<<endl;
        exit(0);
    }
    else{
        cout<<"对master开启服务器监听成功"<<endl;
    }
    return temp;
}

//得到链接
void serverSlave::getConn(){
    
    cout<<"对master开始接收链接"<<endl;
    while(1){
        cout<<"chakan"<<endl;
        len = sizeof(struct sockaddr_in);
        int conn = accept(socketID, (struct sockaddr*) &serverAddr, &len);
        pthread_rwlock_wrlock(&rwlock);
        connList.push_back(conn);
        pthread_rwlock_unlock(&rwlock);
        cout<<"对master新加入一个链接："<<conn<<endl;
        //usleep(100000);
    }
}

//接受数据
bool serverSlave::myRec(int conn, void *buffer){
    //Master和Slave节点通信的服务端
    //分为三种“create ok”， “close", "sentense", "plan"
    size_t size = 0;
    int err = 0;
    size_t index = 0;
    err = recv(conn, &size, sizeof(size_t), 0);
    //cout<<"接受串长度："<<size<<endl;
    if(err <= 0){
        if(ereaConnList(conn)){
          cout<<"删除: "<<conn<<endl;
          close(conn);
        }
        return false;
    }
    while(size){
        err = recv(conn, (char*)buffer + index, size, 0);
        if(err == -1) break;
        else if(err == 0) break;
        size -= err;
        index += err;
    }
    string str((char *)buffer);
    //cout<<"接收数据："<<str<<endl;
    return size == 0;
}

//发送数据
bool serverSlave::mySend(int conn, void* buffer, size_t size){
    
    int err = 0;
    size_t index = 0;
    index = size;
    err = send(conn, &index, sizeof(index), 0);
   // cout<<"发送串长度："<<index<<endl;
    if(err <= -1){
        if(ereaConnList(conn)){
            close(conn);
        }
        return false;
    }
    index = 0;
    while (size) {
        if(size > 4096){
            err = send(conn, (char*)buffer + index, 4096, 0);
        }
        else{
            err = send(conn, (char*)buffer + index, size, 0);
        }
        if(err == -1) break;
        else if(err == 0) break;
        size -= err;
        index += err;
    }
    return size == 0;
}

//加入链表
bool serverSlave::addConnList(int ids){
    pthread_rwlock_wrlock(&rwlock);
    connList.push_back(ids);
    pthread_rwlock_unlock(&rwlock);
    return true;
}

bool serverSlave::mystart(){
    
    createSocket();
    myBind();
    mylisten();
    return true;
}

bool serverSlave::myclose(){
    
    list<int>::iterator it;
    pthread_rwlock_wrlock(&rwlock);
    for(it = connList.begin(); it != connList.end(); it++){
        if(isClose(*it)){
            close(*it);
        }
    }
    pthread_rwlock_unlock(&rwlock);
    close(socketID);
    return true;
}

//查看某个套接字是否关闭
bool serverSlave::isClose(int conn) {
  fd_set rfd;
  FD_ZERO(&rfd);
  FD_SET(conn, &rfd);
  timeval tv = { 0 };
  select(conn+1, &rfd, 0, 0, nullptr);
  if (!FD_ISSET(conn, &rfd))
    return false;
  int n = 0;
  ioctl(conn, FIONREAD, &n);
  return n == 0;
}

//得到套接字列表
list<int> serverSlave:: getConnList() const{
    
    list<int> re;
    pthread_rwlock_rdlock(&rwlock);
    re = connList;
    pthread_rwlock_unlock(&rwlock);
    return re;
}

//得到服务器绑定g端口
unsigned int serverSlave:: getPort() const{
    
    return port;
}
  
//得到初始socketID
int serverSlave::getSocketID() const{
    int idre;
    pthread_rwlock_rdlock(&rwlock);
    idre = socketID;
    pthread_rwlock_unlock(&rwlock);
    return idre;
}

bool serverSlave::ereaConnList(int conn){
    
    pthread_rwlock_wrlock(&rwlock);
    auto it = find(connList.begin(), connList.end(), conn);
    auto end = connList.end();
    pthread_rwlock_unlock(&rwlock);
     if(it == end){
         cout<<"Slave和Master节点之间通信要删除的socket不存在"<<endl;
         return false;
     }
     else{
         pthread_rwlock_wrlock(&rwlock);
         connList.erase(it);
         pthread_rwlock_unlock(&rwlock);
     }
     return true;
}

//开始链接，由于pthread不能传递成员函数，故创建此函数
void* serverStartConn_Master(void* serv) {
    static_cast<serverSlave*>(serv)->getConn();
    return 0;
}

