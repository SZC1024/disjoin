//
//  main.cpp
//  Master
//
//  Created by 周华健 on 2019/12/17.
//  Copyright © 2019 周华健. All rights reserved.
//

#include <iostream>
#include <string>
#include <time.h>
#include <sys/time.h>
#include "client.hpp"
#include "manageToMaster.hpp"
#include "webAPI.hpp"
using namespace std;
double get_wall_time()  
{    	  
    struct timeval time ;    	      
    if (gettimeofday(&time,NULL)){    			  
	return 0;    			  
    }    		  
  return (double)time.tv_sec + (double)time.tv_usec * .0000001;    		      
}    
int main(int argc, const char * argv[]) {
    const char* re;
    create();
 
    string query = "";
    ifstream in("./Query/queryLUBM1");
    string line;
    while(getline(in,line)){
	query += line + '\n';
    }
    double startTime = get_wall_time();
    re = queryToWeb((char*)query.c_str());
    double endTime = get_wall_time();
    cout<<"耗费的时间"<<endTime - startTime<<endl;
    return 0;
}
