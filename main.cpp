//
//  main.cpp
//  Master
//
//  Created by 周华健 on 2019/12/17.
//  Copyright © 2019 周华健. All rights reserved.
//

#include <iostream>
#include <string>
#include "client.hpp"
#include "manageToMaster.hpp"
#include "webAPI.hpp"
using namespace std;
int main(int argc, const char * argv[]) {
    const char* re;
    create();
    
    string query = "";
    ifstream in("./Query/queryLUBM1");
    string line;
    while(getline(in,line)){
	query += line + '\n';
    }
    re = queryToWeb((char*)query.c_str());
    
    return 0;
}
