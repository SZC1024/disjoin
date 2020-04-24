//
//  comClass.cpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/15.
//  Copyright © 2019 周华健. All rights reserved.
//  通信类实现

#include "comClass.hpp"

pthread_rwlock_t rwlock_slave;

server::server(){
    
    port = PORT_SLAVE;
    pthread_rwlock_init(&rwlock_slave, nullptr);
}

server::server(unsigned int port1){
     
    port = port1;
}

//创建初始套接字
int server::createSocket(){
    
    socketID = socket(AF_INET, SOCK_STREAM, 0);
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = inet_addr(IP);
    cout<<"对服务器节点创建套接字成功: "<<socketID<<endl;
    return 1;
}

//绑定端口
int server::myBind(){
    
    auto temp =  ::bind(socketID, (struct sockaddr*) &serverAddr, sizeof(serverAddr));
    if(temp == -1){
        cout<<"服务器绑定端口失败"<<endl;
        exit(0);
    }
    else{
        cout<<"服务器绑定端口成功"<<endl;
    }
    return temp;
    return 1;
}

//监听端口
int server::mylisten(){
    
    int temp;
    len = sizeof(serverAddr);
    temp = listen(socketID, 1024); //最多监听1024个队列
    
    if(temp == -1){
        
        cout<<"服务器监听失败"<<endl;
        exit(0);
    }
    else{
        cout<<"对slave开启服务器监听成功"<<endl;
    }
    return temp;
}

void server::getConn(){
    
    while(1){
        int conn = accept(socketID, (struct sockaddr*) &serverAddr, &len);
        pthread_rwlock_wrlock(&rwlock_slave);
        connList.push_back(conn);
        pthread_rwlock_unlock(&rwlock_slave);
        cout<<"对slave新加入一个链接："<<conn<<endl;
    }
}

//接受数据
bool server::myRec(int conn, void *buffer){
    
    size_t size;
    int err;
    size_t index = 0;
    
    err = recv(conn, &size, sizeof(index), 0);
    //memset(buffer, 0, size);
    //cout<<"接受串长度1："<<size<<endl;
    if(err <= 0){
        if(ereaConnList(conn)){
            cout<<"删除:"<<conn<<endl;
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
    //printf("收到的结果：%s",buffer);
    return size == 0;
}

//发送数据
bool server::mySend(int conn, void* buffer, size_t size){
    
    int err;
    size_t index = 0;
    index = size;
    err = send(conn, &index, sizeof(index), 0);
    //cout<<"发送串长度1："<<index<<endl;
    index = 0;
    if(err <= 0){
        if(ereaConnList(conn)){
            close(conn);
        }
        return false;
    }
    index = 0;
    while (size) {
        if(size > 4096){
            err = send(conn, (char*) buffer + index, 4096, 0);
        }
        else{
            err = send(conn,(char*) buffer + index, size, 0);
        }
        if(err == -1) break;
        else if(err == 0) break;
        size -= err;
        index += err;
    }
    //printf("发送的结果%s", buffer);
    return size == 0;
}


bool server::mystart(){
    
    
    createSocket();
    myBind();
    mylisten();
    cout<<"对slave节点开启服务器成功，开始监听"<<endl;
    return true;
}

bool server::myclose(){
    pthread_rwlock_wrlock(&rwlock_slave);
    for(auto it = connList.begin(); it != connList.end(); it++){
        if(isClose(*it)){
            close(*it);
        }
    }
    pthread_rwlock_unlock(&rwlock_slave);
    cout<<"关闭slave对slave服务器"<<endl;
    close(socketID);
    return true;
}

//查看某个套接字是否关闭
bool server::isClose(int conn) {
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
list<int> server:: getConnList() const{
    list<int> re;
    pthread_rwlock_rdlock(&rwlock_slave);
    re = connList;
    pthread_rwlock_unlock(&rwlock_slave);
    
    return re;
}

//得到服务器绑定g端口
unsigned int server:: getPort() const{
    
    return port;
}
  
//得到初始socketID
int server:: getSocketID() const{
    
    return socketID;
}

//删除列表某个元素

bool server::ereaConnList(int conn){
    
    pthread_rwlock_wrlock(&rwlock_slave);
    auto it = find(connList.begin(), connList.end(), conn);
    auto end = connList.end();
    pthread_rwlock_unlock(&rwlock_slave);
    if(it == end){
        cout<<"Slave节点之间通信要删除的socket不存在"<<endl;
        return false;
    }
    else{
        pthread_rwlock_wrlock(&rwlock_slave);
        cout<<"删除list"<<endl;
        connList.erase(it);
        pthread_rwlock_unlock(&rwlock_slave);
    }
    return true;
}

bool server::addConnList(int sid){
    pthread_rwlock_wrlock(&rwlock_slave);
    connList.push_back(sid);
    pthread_rwlock_unlock(&rwlock_slave);
    return true;
}

//开始链接，由于pthread不能传递成员函数，故创建此函数
void* serverStartConn_Slave(void* serv) {
    static_cast<server*>(serv)->getConn();
    return 0;
}
