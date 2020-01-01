//
//  comClassClient.hpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/17.
//  Copyright © 2019 周华健. All rights reserved.
//

#ifndef comClassClient_hpp
#define comClassClient_hpp

#include <stdio.h>
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

using namespace std;

class client{
private:
    string IPServer;
    unsigned int port;
    int socketID;
    sockaddr_in serverAddr;
    
public:
    client();
    client(string ip, unsigned int port1);
    bool createSocket();
    bool myConnect();
    bool mySend(void* buffer, size_t size);
    bool myRec(void* buffer);
    string getIPServer() const;
    unsigned int getPort() const;
    int getSocketID() const;
    void myclose();
};
#endif /* comClassClient_hpp */
