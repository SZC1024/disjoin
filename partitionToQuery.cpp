//
//  partitionToQuery.cpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//

#include "partitionToQuery.hpp"

 partitionToSub:: partitionToSub(size_t id, unordered_map<size_t, string> unMap, unordered_map<size_t, size_t>unMapre, unordered_map<size_t, vector<string> > unMap_name){
    ID = id;
    cout<<"分区内查看个数："<<unMap.size()<<endl;
    for(auto i = unMap.begin(); i != unMap.end(); i++){
        vector<string> tempVec;
        tempVec.push_back(i->second);
        subQuery* temp = new subQuery(i->first, 0, 0, 0, tempVec, unMap_name[i->first], unMapre[i->first]);
        subRef[i->first] = temp;
        subQueryVec.push_back(i->second);
    }
}

//变更子查询计划
bool  partitionToSub::alterSubPlan(vector<structPlan> &temp){
    
    subPlan.swap(temp);
    return true;
}

//加子查询
bool partitionToSub::addSubref(size_t id, subQuery *sub){
    
    if(subRef.find(id) != subRef.end()){
        subRef[id] = sub;
        return true;
    }
    else{
        cout<<"该子查询已经存在"<<endl;
    }
    return false;
}

//得到子查询计划
vector<structPlan> partitionToSub:: getSubPlan(){
    
    return subPlan;
}

//
subQuery* partitionToSub:: getSubQuery(size_t id){
    
    if(subRef.find(id) != subRef.end()){
        
        return subRef[id];
    }
    cout<<"该子查询不存在此分区，因此无法返回"<<endl;
    subQuery* err = new subQuery();
    
    return err;
}

//得到子查询变量
vector<string> partitionToSub::getSubQueryVec(){
    
    return subQueryVec;
}

//得到子查询变量个数
size_t partitionToSub::getSubQueryVecSize(){
    
    return subQueryVec.size();
}


//得到分区所有子查询的ID
vector<size_t> partitionToSub::getAllSubID(){
    
    vector<size_t> id;
    unordered_map<size_t, subQuery*>::iterator it;
    for(it = subRef.begin(); it != subRef.end(); it++){
        size_t temp = (*it).first;
        id.push_back(temp);
    }
    return id;
}

//根据ID得到子查询语句
vector<string> partitionToSub::getSubQueryStr(size_t id){
    
    subQuery* temp = subRef[id];
    
    return temp->getQueryVec();
}

//根据ID得到变量名
vector<string> partitionToSub::getSubQueryName(size_t id){
    
    subQuery* temp = subRef[id];
    
    return temp->getValNameVec();
}

//得到查询计划数组元素大小
size_t partitionToSub::getSubPlanSize(){
    
    return subPlan.size();
}
