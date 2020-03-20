//
//  main.cpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/13.
//  Copyright © 2019 周华健. All rights reserved.
//

#include <iostream>
#include "manageToSlave.hpp"
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
      cout<<"开始"<<endl;
      pthread_t slave1;
      pthread_t master1;
      
      pthread_create(&slave1, nullptr, serverStart_Slave, (void*) manage);
      pthread_detach(slave1);
      
      pthread_create(&master1, nullptr, serverStart_Master, (void*) manage);
      pthread_detach(master1);
      
    while(!manage->is_NULL_ServerToSlave() && !manage->is_NULL_ServerToMaster()){
       // cout<<"开始监听中"<<endl;
    }
    delete manage;
    return  0;
}
