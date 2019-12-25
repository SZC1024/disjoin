//
//  manageToMaster.cpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//

#include "manageToMaster.hpp"

size_t manageToMaster::queryId  = 1;

manageToMaster::manageToMaster(){
    
    ifstream in("./host");
    if(! in){
        cout<<""<<endl;
    }
    in>>ID;
    size_t id;
    string str;
    while(in>>id>>str){
        ipRef[id] = str;
    }
}

size_t manageToMaster:: addQuery(string str){
    cout<<"查询ID   ：              "<<queryId<<endl;  
    size_t qu = queryId;
    generalQuery* query = new generalQuery(qu, str);
    
    queryRef[queryId] = query;
     queryId ++;
    return queryId - 1;
}

bool manageToMaster:: removeQuery(size_t id1){
    
    queryRef.erase(id1);
    
    return true;
}

unordered_map<size_t, string> manageToMaster:: getIpRef(){
    
    unordered_map<size_t, string> un(ipRef);
    
    return un;
}

size_t manageToMaster::getID(){
    
    return ID;
}

//执行查询
vector<vector<size_t> > manageToMaster:: exeuteQuery(size_t id){
    
    generalQuery* temp = queryRef[id];
    
    temp->mystart();
    
    vector<vector<size_t> > temp1 = temp->getResult();
    
    return temp1;
}

//得到查询个数
size_t manageToMaster::getQuerySize(){
    
    size_t re;
    re = queryRef.size();
    return re;
}

//得到节点个数，包含master节点
size_t manageToMaster::getHostNum(){
    size_t num;
    num = ipRef.size();
    return num;
}
