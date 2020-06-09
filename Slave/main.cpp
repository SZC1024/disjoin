//
//  main.cpp
//  garphDatabase
//
//  Created by songzc on 2019/12/13.
//  Copyright © 2019 songzc. All rights reserved.
//

#include <iostream>
#include <unistd.h>
#include <fstream>
#include "manageToSlave.hpp"
using namespace std;
typedef struct MEMPACKED         //定义一个mem occupy的结构体  
{
	char name1[20];      //定义一个char类型的数组名name有20个元素  
	unsigned long MemTotal;
	char name2[20];
	unsigned long MemFree;
	char name3[20];
	unsigned long Buffers;
	char name4[20];
	unsigned long Cached;
	char name5[20];
	unsigned long SwapCached;
}MEM_OCCUPY;
typedef struct CPUPACKED         //定义一个cpu occupy的结构体  
{
	char name[20];      //定义一个char类型的数组名name有20个元素  
	unsigned int user; //定义一个无符号的int类型的user  
	unsigned int nice; //定义一个无符号的int类型的nice  
	unsigned int system;//定义一个无符号的int类型的system  
	unsigned int idle; //定义一个无符号的int类型的idle  
	unsigned int lowait;
	unsigned int irq;
	unsigned int softirq;
}CPU_OCCUPY;
void get_memoccupy(MEM_OCCUPY* mem) //对无类型get函数含有一个形参结构体类弄的指针O  
{
	FILE* fd;
	char buff[256];
	MEM_OCCUPY* m;
	m = mem;
	fd = fopen("/proc/meminfo", "r");
	fgets(buff, sizeof(buff), fd);
	sscanf(buff, "%s %lu ", m->name1, &m->MemTotal);
	fgets(buff, sizeof(buff), fd);
	sscanf(buff, "%s %lu ", m->name2, &m->MemFree);
	fgets(buff, sizeof(buff), fd);
	sscanf(buff, "%s %lu ", m->name3, &m->Buffers);
	fgets(buff, sizeof(buff), fd);
	sscanf(buff, "%s %lu ", m->name4, &m->Cached);
	fgets(buff, sizeof(buff), fd);
	sscanf(buff, "%s %lu", m->name5, &m->SwapCached);
	fclose(fd);     //关闭文件fd  
}
int get_cpuoccupy(CPU_OCCUPY* cpust) //对无类型get函数含有一个形参结构体类弄的指针O  
{
	FILE* fd;
	char buff[256];
	CPU_OCCUPY* cpu_occupy;
	cpu_occupy = cpust;
	fd = fopen("/proc/stat", "r");
	fgets(buff, sizeof(buff), fd);
	sscanf(buff, "%s %u %u %u %u %u %u %u", cpu_occupy->name, &cpu_occupy->user, &cpu_occupy->nice, &cpu_occupy->system, &cpu_occupy->idle, &cpu_occupy->lowait, &cpu_occupy->irq, &cpu_occupy->softirq);
	fclose(fd);
	return 0;
}
int cal_cpuoccupy(CPU_OCCUPY* o, CPU_OCCUPY* n, double& cpuidle, double& cpuuse)
{
	unsigned long od, nd;
	double cpu_use = 0;
	od = (unsigned long)(o->user + o->nice + o->system + o->idle + o->lowait + o->irq + o->softirq);//第一次(用户+优先级+系统+空闲)的时间再赋给od  
	nd = (unsigned long)(n->user + n->nice + n->system + n->idle + n->lowait + n->irq + n->softirq);//第二次(用户+优先级+系统+空闲)的时间再赋给od  
	double sum = nd - od;
	double idle = n->idle - o->idle;
	cpu_use = idle / sum;
	cpuidle = cpu_use;
	idle = n->user + n->system + n->nice - o->user - o->system - o->nice;
	cpu_use = idle / sum;
	cpuuse = cpu_use;
	return 0;
}


int main(int argc, const char * argv[]) {
    
    manageToSlave* manage = nullptr;
    if(manage == nullptr){
        cout<<"创建管理类"<<endl;
        manage = new manageToSlave();
    }
    else{
        cout<<"管理类存在，重新创建"<<endl;
        if( manage->is_NULL_ServerToMaster()|| manage->is_NULL_ServerToSlave()){
            delete manage;
            manage = new  manageToSlave();
        }
    }
    //开始监听
	cout << "开始" << endl;
	pthread_t slave1;
	pthread_t master1;

	pthread_create(&slave1, nullptr, serverStart_Slave, (void*)manage);
	pthread_detach(slave1);

	pthread_create(&master1, nullptr, serverStart_Master, (void*)manage);
	pthread_detach(master1);

	ofstream fout("system-stat.txt");
	MEM_OCCUPY mem_stat;
	CPU_OCCUPY cpu_stat1;
	CPU_OCCUPY cpu_stat2;
	for (unsigned long long i = 0;; i += 1) {
		//获取内存
		get_memoccupy((MEM_OCCUPY*)&mem_stat);
		fout << i << "s : ";
		fout << "[MemTotal] = " << mem_stat.MemTotal;
		fout << "\t[MemFree] = " << mem_stat.MemFree;
		fout << "\t[Buffers] = " << mem_stat.Buffers;
		fout << "\t[Cached] = " << mem_stat.Cached;
		fout << "\t[SwapCached] = " << mem_stat.SwapCached;
		//第一次获取cpu使用情况
		get_cpuoccupy((CPU_OCCUPY*)&cpu_stat1);
		usleep(1000000);//1秒
		//第二次获取cpu使用情况
		get_cpuoccupy((CPU_OCCUPY*)&cpu_stat2);
		//计算cpu使用率
		double cpu_idle;
		double cpu_use;
		cal_cpuoccupy((CPU_OCCUPY*)&cpu_stat1, (CPU_OCCUPY*)&cpu_stat2, cpu_idle, cpu_use);
		fout << "\tcpu_use(idle) = " << cpu_idle;
		fout << "\tcpu_use(user+system+nice) = " << cpu_use << endl;
		if(!(!manage->is_NULL_ServerToSlave() && !manage->is_NULL_ServerToMaster())) break;
	}
    
    delete manage;
    return  0;
}
