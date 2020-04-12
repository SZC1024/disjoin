//
//  comClassClient.cpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/17.
//  Copyright © 2019 周华健. All rights reserved.
//  slave节点客户端

#include "comClassClient.hpp"
//client

client::client(string ip, unsigned int port1){
    
    IPServer = ip;
    port = port1;
}

//客户端创建ID
bool client::createSocket(){
    
    socketID = socket(AF_INET, SOCK_STREAM, 0);
    
    memset(&serverAddr, 0, sizeof(serverAddr));
    
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = inet_addr(IPServer.c_str());
    
    return true;
}

//链接服务端
bool client::myConnect(){
    
    if(connect(socketID, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0){
        cout<<"客户端链接失败："<<endl;
        exit(0);
    }
    return true;
}

//发送数据
bool client::mySend(void* buffer, size_t size){
    
    int err;
    size_t index;
    index = size;
    err = send(socketID, &index, sizeof(size_t), 0);
   // cout<<"发送串长度："<<err<<endl;
    index = 0;
    while (size) {
        if(size > 1024000000){
            err = send(socketID, (char *)buffer + index, 1024000000, 0);
        }
        else{
            err = send(socketID, (char *)buffer + index, size, 0);
        }
        if(err == -1) break;
        else if(err == 0) break;
        size -= err;
        index += err;
    }
    return size == 0;
}

//接收数据
bool client::myRec(void *buffer){
    
    size_t size;
    int err;
    size_t index = 0;
    
    err = recv(socketID, &size, sizeof(size_t), 0);
    memset(buffer, 0, size);
    //cout<<"接受串长度："<<err<<endl;
    if(err <= 0){
        cout<<"客户端接收失败"<<endl;
        close(socketID);
        return false;
    }
    while(size){
        err = recv(socketID, (char*)buffer + index, size, 0);
        if(err == -1) break;
        else if(err == 0) break;
        size -= err;
        index += err;
    }
    
    return size == 0;
}

//得到服务器ip
string client:: getIPServer() const{
    
    return IPServer;
}

//得到服务器端口
unsigned int client::getPort() const{
    
    return port;
}

//得到套接字
int client::getSocketID() const{
    
    return socketID;
}

//关闭
void client::myclose(){
    
    close(socketID);
}
