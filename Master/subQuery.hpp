//  每个子查询

#ifndef subQuery_hpp
#define subQuery_hpp

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string.h>
#include <algorithm>

using namespace std;

class subQuery{
private:
    size_t ID;             //子查询类编号
    size_t parentIDLeft;   //左父类编号，默认是0，代表此对象由查询语句创建
    size_t parentIDRight;  //右父类编号，默认是0
    int type;           //结合方式，默认0，1代表union得到，2代表join得到
    vector<string> queryStrVec;    //查询语句数组，
    size_t queryValNum;            //该查询类对象结果集含有多少个变量
    vector<string> valNameVec;     //变量名数组
    size_t resultCount;         //结果数量
    
public:
    subQuery();
    subQuery(size_t id, vector<string> query, vector<string> name, size_t count);
    subQuery(size_t id, size_t left, size_t right, int type1, vector<string> query, vector<string>name, size_t count);
    subQuery(subQuery& query);
    subQuery& subquery(subQuery& query);
    subQuery* Union(subQuery query, size_t id);     //查询类合并
    subQuery* Join(subQuery query, size_t id);     //查询类Join
    bool findCommonValName(subQuery query, vector<string>& C);//发现两个查询语句的相同变量， A，B分别代表两查询的相同变量的位置
    vector<string> getQueryVec() const;    //得到查询语句
    size_t getQueryValNum() const;            //得到查询语句变量个数
    vector<string> getValNameVec() const;  //得到查询语句变量名
    size_t getCount() const;  //得到查询语句的值
    size_t getID() const;    //
    size_t getParentLeft() const;
    size_t getpParentRight() const;
    int getType() const;
    subQuery& operator= (subQuery& query);   //赋值运算符
    int isCommon(subQuery query);  //判断两个查询类是不是相同
    int setID(size_t id);
    int setParentID(size_t left, size_t right);
    int setType(int tyep1);    
};

#endif /* subQuery_hpp */
