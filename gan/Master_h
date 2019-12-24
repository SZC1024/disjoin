/* MasterToWeb.h*/
/* 提供给上层web的接口 */

#ifndef MASTERTOWEB_H
#define MASTERTOWEB_H

#include "./Master.h"
#include <iostream>
#include <vector>
#include <string.h>
#include "./Client.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/shm.h>

using namespace std;

extern Client *client;

extern char *resultEnd;

extern "C" {

const char *queryToWeb(char *querySen);    //查询语句返回

int create();     //加载数据库

void closeDb();      //关闭数据库

void resFree();    //回收内存

vector <string> queryComposeToVec(char *querySen, string &subQueryFile);   //查询语句分解成子查询语句

vector <string> unionToRow(char **buffer, size_t row);    //单个查询语句去重

size_t lineStringNum(string temp);
}
#endif
