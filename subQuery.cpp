//
//  subQuery.cpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//

#include "subQuery.hpp"

#define MAX_VAL_NUM 1024
using namespace std;
/* 默认构造函数*/
subQuery::subQuery(){
    queryValNum = 0;
    ID = 0;
    parentIDLeft = 0;
    parentIDRight = 0;
    type = 0;
}

/* 拷贝构造函数,默认不能有拷贝构造函数*/

subQuery::subQuery(subQuery& query){
    
    vector<string> queryVec;
    vector<string> queryName;
    
    //赋值ID
    ID = query.getID();
    parentIDLeft = query.getParentLeft();
    parentIDRight = query.getpParentRight();
    type = query.getType();
    
    size_t num = query.getQueryValNum();
    
    //赋值变量个数
    if(num == 0) {
        queryValNum = num;
        return;
     }
     else{
         queryValNum = num;
    }
    
    //赋值查询语句
    vector<string> temp = query.getQueryVec();
    queryStrVec.swap(temp);
    
    //赋值变量名
    temp = query.getValNameVec();
    valNameVec.swap(temp);
    
    //赋值属性值
    resultCount = query.getCount();
}
 

//以查询语句创建queryclass
subQuery::subQuery(size_t id, vector<string> query, vector<string> name, size_t count){
    
    size_t nameVecNum = name.size();
    
    //异常处理
    if(query.size() == 0 || query.size() > 1){
        cout<<"创建子查询类失败，查询语句为空或大于一条, ID: "<< id<<endl;
        return;
    }
    if(name.size() == 0){
        cout<<"创建子查询类失败，查询变量名不能为空, ID: "<<id<<endl;
        return;
    }
    
    //赋值ID
    ID = id;
    
    //赋值血缘信息
    parentIDRight = 0;
    parentIDLeft = 0;
    type = 0;
    
    //赋值查询语句
    queryStrVec.swap(query);
    
    //赋值变量名
    valNameVec.swap(name);
    
    //赋值查询语句变量个数
    queryValNum = nameVecNum;
         
    //执行策略初步策略，查看结果
    
    
    resultCount = count;
}


//创建queryclass
 subQuery::subQuery(size_t id, size_t left, size_t right, int type1, vector<string> query, vector<string>name, size_t count){
    
     size_t nameVecSize = name.size();
     
     //赋值ID
     ID = id;
     
     //赋值血缘关系
     parentIDRight = right;
     parentIDLeft = left;
     type = type1;
     
    //赋值查询语句
    query.swap(queryStrVec);
    
    //赋值变量个数
    queryValNum = nameVecSize;
    
    //赋值变量名数组
    valNameVec.swap(name);

    //赋值属性值
     resultCount = count;
}

//赋值运算符重载
subQuery& subQuery::operator= (subQuery& query){
    
    ID = query.getID();
    parentIDRight = query.getpParentRight();
    parentIDLeft = query.getParentLeft();
    type = query.getType();
     
    //赋值查询语句
    vector<string> temp =   query.getQueryVec();
    queryStrVec.swap(temp);
    
    //赋值变量h个数
    queryValNum = query.getQueryValNum();
    
    //赋值变量名数组
    temp = query.getValNameVec();
    valNameVec.swap(temp);
    
    //赋值属性值
    resultCount = query.getCount();
    
    return *this;
}

//获取查询语句vec
vector<string> subQuery::getQueryVec(){
    
    return queryStrVec;
}

//获取查询变量个数
size_t subQuery::getQueryValNum(){
    
    return queryValNum;
}
 
//获取变量名
vector<string> subQuery::getValNameVec(){
    
    return valNameVec;
}

//获取结果值
size_t subQuery::getCount(){
    
    return resultCount;
}

//获取ID
size_t subQuery::getID(){
    
    return ID;
}

//获取类型
int subQuery::getType(){
    
    return type;
}

//获取左父ID
size_t subQuery::getParentLeft(){
    
    return parentIDLeft;
}

//获取右父ID
size_t subQuery::getpParentRight(){
    
    return parentIDRight;
}



/*
 查找两个查询类的相同变量
 A， B数组代表变量在valNameVec的位置
*/
bool subQuery::findCommonValName(subQuery& query, vector<string>& C){
    
    int flag;
    int flag1 = 0;
    size_t id = query.getID();
    vector<string> name1 = query.getValNameVec();
    
    //id小的合并在前
    if(ID < id){
        flag = 0;
        C.insert(C.begin(), valNameVec.begin(), valNameVec.end());
    }
    else{
        flag = 1;
        C.insert(C.begin(), name1.begin(), name1.end());
    }
    //得到合并以后的新name数组，ID小的在前
     //例如Id1: A B；id2: B C
    // 合并i以后就是A B C
    if(flag == 0){
        for(size_t i = 0; i < name1.size(); i++){
            vector<string>::iterator it = find(valNameVec.begin(), valNameVec.end(), name1.at(i));
            if(it == valNameVec.end()){
                C.push_back(name1.at(i));
            }
            else{
                flag1 = 1;
            }
        }
    }
    else{
         for(size_t i = 0; i < valNameVec.size(); i++){
             vector<string>::iterator it = find(name1.begin(), name1.end(), valNameVec.at(i));
             if(it == name1.end()){
                 C.push_back(valNameVec.at(i));
             }
             else{
                 flag1 = 1;
             }
         }
     }
    return flag1;
}
/*
size_t subQuery::findCommonValName(subQuery& query, vector<int>& A, vector<int>& B, vector<string>& C){
    
    int flag;
    size_t id = query.getID();
    vector<string> name1 = query.getValNameVec();
    
    //id小的合并在前
    if(ID < id){
        flag = 0;
        C.insert(C.begin(), valNameVec.begin(), valNameVec.end());
    }
    else{
        flag = 1;
        C.insert(C.begin(), name1.begin(), name1.end());
    }
    
    size_t k = 0;
    
    //找到公共变量位置
    for(int i = 0; i < queryValNum; i++){
        for(int j = 0; j < name1.size(); j++){
            if(valNameVec.at(i) == name1.at(j)) {
                A.push_back(i);
                B.push_back(j);
                k ++;
                break;
            }
        }
    }

    //得到合并以后的新name数组，ID小的在前
    
     //例如Id1: A B；id2: B C
    // 合并i以后就是A B C
     
    if(flag == 0){
        for(size_t i = 0; i < name1.size(); i++){
            vector<string>::iterator it = find(valNameVec.begin(), valNameVec.end(), name1.at(i));
            if(it == valNameVec.end()){
                C.push_back(name1.at(i));
            }
        }
    }
    else{
         for(size_t i = 0; i < valNameVec.size(); i++){
             vector<string>::iterator it = find(name1.begin(), name1.end(), valNameVec.at(i));
             if(it == name1.end()){
                 C.push_back(valNameVec.at(i));
             }
         }
     }
    return k;
}
*/

/*
  判断两个查询类是不是相同
  相同返回1
  不同返回-1
  返回2代表是A B C 和 c b a 这种类型的
  a b c 和 c b a是相同的
*/
int subQuery::isCommon(subQuery& query){
    vector<string> name = query.getValNameVec();
    int flag = 0;
    size_t num = query.getQueryValNum();
    if(num != queryValNum) return -1;
    for(int i = 0; i < num; i++){
        for(int j = 0; j < num; j++){
            if(name.at(i) == valNameVec.at(j)){
                if(i != j) flag = 1;
                break;
            }
            if(j == num - 1) return -1;
        }
    }
    if(flag == 1) return 2;
      else
          return 1;
}
   
/*
 两个查询类合并去重，返回合并以后的结果集大小.
 返回-1代表两个结果集不能合并
 目前只考虑a b c一一对齐
 a b c和 c b a这种不进行合并
*/

/* template<typename T>
struct VectorHash {
    size_t operator()(const std::vector<T>& v) const {
        std::hash<T> hasher;
        size_t seed = 0;
        for (auto& i : v) {
            seed ^= hasher(i) + 0x9e3779b9 + (seed<<6) + (seed>>2);
        }
        return seed;
    }
};
*/

subQuery* subQuery::Union(subQuery& query, size_t id){
    
    vector<string> name = valNameVec;
    vector<string> querystr = queryStrVec;
    vector<vector<size_t> >newVal;
    subQuery* errorRe = new subQuery();
    int temp = isCommon(query);
    if(temp == -1){
        cout << "结果集变量名不一致：ID：" << ID << "  ID:" << query.getID() << endl;
        return errorRe;
    }
    
    if(temp == 2){
        cout << "结果集可以合并，但是目前无法处理, ID:" << ID <<"  ID:"<<query.getID()<<endl;
        return errorRe;
    }
    delete errorRe;
    
    //union两个相同的子查询
    size_t count = resultCount + query.getCount() ;
    
    subQuery* re = new subQuery(id, ID, query.getID(), 1, querystr, name, count);
    return re;
}

/*
结果集join
*/
subQuery* subQuery::Join(subQuery& query, size_t id){
 
  //相同变量的列号
    vector<int> A;
    vector<int> B;
  //返回变量
    vector<string> queryStrRe;
    vector<string> nameRe;
    size_t countRe;
  //query值获取
    vector<string> name = query.getValNameVec();
    vector<string> queryStr = query.getQueryVec();
    size_t count = query.getCount();
    
 //异常返回
   subQuery* errorRe = new subQuery();
  //查找两个查询类的公共变量
    if( !findCommonValName(query, nameRe)){
        cout<<"两个变量无法join"<<endl;
        return errorRe;
    }
  
    delete errorRe;
    //实现join策略
    queryStrRe.swap(queryStr);
    
    if(count > resultCount){
        countRe = resultCount;
    }
    else{
        countRe = count;
    }
    subQuery* re = new subQuery(id, ID, query.getID(), 2, queryStrRe, nameRe, countRe);
    return re;
}

