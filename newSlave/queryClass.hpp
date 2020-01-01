//
//  queryClass.hpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/13.
//  Copyright © 2019 周华健. All rights reserved.
//

#ifndef queryClass_hpp
#define queryClass_hpp
/*  queryClass.h */
/*
   实现一个子查询类
*/

#include<iostream>
#include<stdlib.h>
#include<algorithm>
#include<vector>
#include<string.h>
#include "common.h"
#include<unordered_set>
//#include "../TripleBit/tripleBitQueryAPI.h"
using namespace std;

#define MAX_VAL_NUM 1024
class queryClass{
    
    private:
        size_t ID;             //子查询类编号
        size_t parentIDLeft;   //左父类编号，默认是0，代表此对象由查询语句创建
        size_t parentIDRight;  //右父类编号，默认是0
        int type;           //结合方式，默认0，1代表union得到，2代表join得到
        vector<string> queryStrVec;    //查询语句数组，
        size_t queryValNum;            //该查询类对象结果集含有多少个变量
        vector<string> valNameVec;     //变量名数组
        vector<vector<size_t> > valueVec; //值，行列值与变量名对应
    
    public:
        queryClass();   //默认构造函数
        queryClass(vector<string> queryVec, vector<string> nameVec,vector<vector<size_t> > valVec, size_t id, size_t parentleft, size_t parentright, int type1); // 全套构造函数
        queryClass(vector<string> queryVec, vector<string> nameVec, size_t id);   //query为查询语句，name为变量名
        queryClass(const queryClass& query);  //拷贝构造函数
        queryClass* Union(queryClass query, size_t id);     //查询类合并
        queryClass* Join(queryClass query, size_t id);     //查询类Join
        //vector<string>& Union(queryClass& query);
        size_t findCommonValName(queryClass query, vector<size_t>& A, vector<size_t>& B, vector<string>& C);//发现两个查询语句的相同变量， A，B分别代表两查询的相同变量的位置
        vector<string> getQueryVec() const;    //得到查询语句
        size_t getQueryValNum() const;            //得到查询语句变量个数
        vector<string> getValNameVec() const;  //得到查询语句变量名
        vector<vector<size_t> > getValueVec() const;  //得到查询语句的值
        size_t getID() const;
        size_t getParentLeft() const;
        size_t getpParentRight() const;
        int getType() const;
        queryClass& operator= (const queryClass& query);   //赋值运算符
        int isCommon(queryClass query);  //判断两个查询类是不是相同
        int setID(size_t id);
        int setParentID(size_t left, size_t right);
        int setType(int tyep1);
};

#endif /* queryClass_hpp */
