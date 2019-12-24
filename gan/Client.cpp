//
// Created by peng on 19-11-28.
//

#include "Client.h"
#include <iostream>

Client::Client(SOCKADDR *server) {
	for (int i = 0; i < SLAVE_NUM; ++i) {
		servaddr[i] = server[i];
		socketid[i] = socket(AF_INET, SOCK_STREAM, 0);
	}
}

int Client::myConnect() {
	for (int i = 0; i < SLAVE_NUM; ++i) {
		if (connect(socketid[i], (struct sockaddr *) &(servaddr[i]), sizeof(servaddr)) < 0) {
			perror("connect");
			return 1;
		}
	}
	return 0;
}

int Client::mySend(const char *buffer, size_t len) {
	for (int i = 0; i < SLAVE_NUM; ++i) {
		send(socketid[i], buffer, len, 0);
	}
	return 0;
}

int Client::myRecv(char **buffer, size_t len) {
	size_t count = 0;
	size_t counts[SLAVE_NUM];
	for (int i = 0; i < SLAVE_NUM; ++i) {
		count = 0;
		recv(socketid[i], &counts[i], sizeof(size_t), 0);
		std::cout << "slave " << i << " send buffer size: " << counts[i] << std::endl;
		while (count < counts[i]) {
			count += recv(socketid[i], buffer[i] + count, len - count, 0);
		}
		std::cout << "read from slave: " << i << " read count:" << count << std::endl;
	}
	return 0;
}

int Client::myClose() {
	for (int i = 0; i < SLAVE_NUM; ++i) {
		close(socketid[i]);
	}
	return 0;
}
