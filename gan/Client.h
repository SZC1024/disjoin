//
// Created by peng on 19-11-28.
//

#ifndef PROJECT_CLIENT_H
#define PROJECT_CLIENT_H


#define SLAVE_NUM 3

#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/shm.h>

typedef struct sockaddr_in SOCKADDR;

class Client {
	int socketid[SLAVE_NUM];
	SOCKADDR servaddr[SLAVE_NUM];

public:
	Client(SOCKADDR server[SLAVE_NUM]);
	
	int myConnect();
	
	int mySend(const char *buffer, size_t len);
	
	int myRecv(char **buffer, size_t len);
	
	int myClose();
};


#endif //PROJECT_CLIENT_H
