#include "queryClass.hpp"
//#define _DEBUG_FOR_SZC_ 1
//#define MAX_VAL_NUM 1024
#include<iostream>
#include<stdlib.h>
#include<algorithm>
#include<vector>
#include<string.h>
#include<unordered_set>
using namespace std;

/* 默认构造函数*/
queryClass::queryClass(){
    queryValNum = 0;
    ID = 0;
    parentIDLeft = 0;
    parentIDRight = 0;
    type = 0;
}

/* 拷贝构造函数,默认不能有拷贝构造函数*/

queryClass::queryClass(const queryClass& query){
    
    vector<string> queryVec;
    vector<string> queryName;
    vector<vector<size_t> > value;
    
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
     
    //读取query对象变量
    value = query.getValueVec();
    
    //赋值查询语句
    vector<string> temp = query.getQueryVec();
    queryStrVec.swap(temp);
    
    //赋值变量名
    temp = query.getValNameVec();
    valNameVec.swap(temp);
    
    //赋值属性值
    vector<vector<size_t>> temp2 = query.getValueVec();
    valueVec.swap(temp2);
}
 

//以查询语句创建queryclass并执行原始子查询
queryClass::queryClass(vector<string> queryVec, vector<string> nameVec, size_t id){
    
    size_t queryVecNum = queryVec.size();
    size_t nameVecNum = nameVec.size();
    
    //异常处理
    if(queryVec.size() == 0 || queryVec.size() > 1){
        cout<<"创建子查询类失败，查询语句为空或大于一条, ID: "<< id<<endl;
        return;
    }
    if(nameVec.size() == 0){
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
    queryStrVec.swap(queryVec);
    
    //赋值变量名
    valNameVec.swap(nameVec);
    
    //赋值查询语句变量个数
    queryValNum = nameVecNum;
         
    //执行查询语句得到结果
    for(size_t i = 0; i < queryVecNum; i ++){
        vector<unsigned int> result;
        
        //此处调用查询语句API得到结果
        result = search(queryStrVec.at(i).c_str());
        //输出结果调试
        cout<<"查询语句结果，ID:"<<ID<<endl;
       /* for(size_t i = 0; i < result.size(); i++){
           cout<<result.at(i)<<" ";
           if(i % queryValNum == 0) cout<<" "<<endl;
        }*/
        
        cout<<"得到了结果"<<endl;
        
        //处理API查询结果
        if(result.empty() || result.at(0) == -1) return;
        
        for(auto it = result.begin(); it < result.end(); it = it + queryValNum){
           vector<size_t> line(it, it + queryValNum);
           valueVec.push_back(line);
        }
        cout << "结果条数：" << valueVec.size() << endl;
    }
}


//创建queryclass
 queryClass::queryClass(vector<string> queryVec, vector<string> nameVec,vector<vector<size_t> > valVec, size_t id, size_t parentleft, size_t parentright, int type1){
    
     size_t nameVecSize = nameVec.size();
     
     //赋值ID
     ID = id;
     
     //赋值血缘关系
     parentIDRight = parentright;
     parentIDLeft = parentleft;
     type = type1;
     
    //赋值查询语句
    queryStrVec.swap(queryVec);
    
    //赋值变量个数
    queryValNum = nameVecSize;
    
    //赋值变量名数组
    valNameVec.swap(nameVec);

    //赋值属性值
    valueVec.swap(valVec);
}

//赋值运算符重载
queryClass& queryClass::operator= (const queryClass& query){
    
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
    vector<vector<size_t>> val = query.getValueVec();
    valueVec.swap(val);
    
    return *this;
}

//获取查询语句vec
vector<string> queryClass::getQueryVec() const{
    
    return queryStrVec;
}

//获取查询变量个数
size_t queryClass::getQueryValNum() const {
    
    return queryValNum;
}
 
//获取变量名
vector<string> queryClass::getValNameVec() const{
    
    return valNameVec;
}

//获取结果值
vector<vector<size_t> > queryClass::getValueVec() const{
    
    return valueVec;
}

//获取ID
size_t queryClass::getID() const{
    
    return ID;
}

//获取类型
int queryClass::getType() const{
    
    return type;
}

//获取左父ID
size_t queryClass::getParentLeft() const{
    
    return parentIDLeft;
}

//获取右父ID
size_t queryClass::getpParentRight() const{
    
    return parentIDRight;
}



/*
 查找两个查询类的相同变量
 A， B数组代表变量在valNameVec的位置
 因为这个函数被调用得到的返回值只用来当作bool使用了，所以暂时将此函数判断依据同步为master端的判断依据
*/
size_t queryClass::findCommonValName(queryClass query, vector<size_t> &A, vector<size_t> &B, vector<string> &C) {
    
    int flag;
    size_t id = query.getID();
    vector<string> name1 = query.getValNameVec();
    
    //id小的合并在前
    if(ID < id){
        flag = 0;
        C.insert(C.begin(), valNameVec.begin(), valNameVec.end());
    }else{
        flag = 1;
        C.insert(C.begin(), name1.begin(), name1.end());
    }
    
    size_t k = 0;
    
    //找到公共变量位置
    for(size_t i = 0; i < queryValNum; i++){
        for(size_t j = 0; j < name1.size(); j++){
            if(valNameVec.at(i) == name1.at(j)) {
                A.push_back(i);
                B.push_back(j);
                k ++;
                break;
            }
        }
    }

#ifdef _DEBUG_FOR_SZC_
	cout << "valNameVec:";
	for (auto a : valNameVec) {
		cout << " " << a;
	}
	cout << endl;
	cout << "name1:";
	for (auto a : name1) {
		cout << " " << a;
	}
	cout << endl;
#endif // _slave_debug_for_szc_


    //得到合并以后的新name数组，ID小的在前
    /*
     例如Id1: A B；id2: B C
     合并i以后就是A B C
     */
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

/*
  判断两个查询类是不是相同
  相同返回1
  不同返回-1
  返回2代表是A B C 和 c b a 这种类型的
  a b c 和 c b a是相同的
*/
int queryClass::isCommon(queryClass query){
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

queryClass* queryClass::Union(queryClass query, size_t id){
    
    vector<string> name = valNameVec;
    vector<string> querystr = queryStrVec;
    vector<vector<size_t> >newVal;
    queryClass* errorRe = new queryClass();
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
    
    //利用set去重机制
    unordered_set<vector<size_t>, VectorHash<size_t> > res{
        valueVec.begin(),
        valueVec.end()
    };
    vector<vector<size_t> > val = query.getValueVec();
    res.insert(val.begin(), val.end());
    vector<vector<size_t> > val1(res.begin(), res.end());
    newVal.swap(val1);
    
    queryClass* re = new queryClass(querystr, name, newVal, id, ID, query.getID(), 1);
    return re;
}

/*
结果集join
*/
//比较函数
bool compare(VectorCompare A, VectorCompare B){//A小于B
    vector<size_t> aname = A.name;
    vector<size_t> bname = B.name;

	vector<size_t>::iterator aiter = aname.begin();
	vector<size_t>::iterator biter = bname.begin();
	for (; !((aiter == aname.end()) || (biter == bname.end())); aiter++, biter++) {
		if (A.val.at(*aiter) < B.val.at(*biter)) return true;
		else if (A.val.at(*aiter) > B.val.at(*biter)) return false;
	}
	return false;//c++sort函数中得等于情况必须返回false
    
    //(大于等于)
    //for(size_t i = 0; i < aname.size(); i ++){
    //    size_t indexA = aname.at(i);
    //    size_t indexB = bname.at(i);
    //    if(A.val.at(indexA) > B.val.at(indexB)){
    //        return true;
    //    }
    //    else if(A.val.at(indexA) < B.val.at(indexB)){
    //        return false;
    //    }
    //}
    //return true;
}

bool equal(VectorCompare A, VectorCompare B){
	vector<size_t> aname = A.name;
	vector<size_t> bname = B.name;
	vector<size_t>::iterator aiter = aname.begin();
	vector<size_t>::iterator biter = bname.begin();
	for (; !((aiter == aname.end()) || (biter == bname.end())); aiter++, biter++) {
		if (A.val.at(*aiter) != B.val.at(*biter)) return false;
	}
	return true;
}

//Join
queryClass* queryClass::Join(queryClass query, size_t id){
 
    //相同变量的列号
    vector<size_t> A;
    vector<size_t> B;
    //返回变量
    vector<string> queryStrRe;
    vector<string> nameRe;
    vector<vector<size_t> > valRe;
     //query值获取
    vector<string> name = query.getValNameVec();
    vector<string> queryStr = query.getQueryVec();
    vector<vector<size_t>> val = query.getValueVec();
    
    //异常返回
    queryClass* errorRe = new queryClass();
    //查找两个查询类的公共变量
    size_t count = findCommonValName(query, A, B, nameRe);
    //异常处理
    if(count == 0){
        cout<<"由于没有共同变量，无法join， ID:"<<ID<<"  ID"<<query.getID()<<endl;
        return errorRe;
    }
    delete errorRe;
    
    //实现join
    //先排序
    //第一个查询类排序,this
    VectorCompare temp_1;
    temp_1.name = A;
    vector<VectorCompare> vec1(valueVec.size(), temp_1);
    for(size_t m = 0; m < valueVec.size(); m ++){
        vec1.at(m).val = valueVec.at(m);
    }
    cout << "sort vec1 ..." << endl;
    sort(vec1.begin(), vec1.end(), compare);//按照join key列（由vec1中的name指定），从小到大对每行数据排序
    cout << "sort vec1 end" << endl;
    //第二个查询类排序,query
    VectorCompare temp_2;
    temp_2.name = B;
    vector<VectorCompare> vec2(val.size(), temp_2);
    for(size_t n = 0; n < val.size(); n++){
        vec2.at(n).val = val.at(n);
    }
    cout << "sort vec2 ..." << endl;
    sort(vec2.begin(), vec2.end(), compare);
    cout << "sort vec2 end" << endl;

#ifdef _DEBUG_FOR_SZC_
	cout << ID << " value:" << endl;
	for (int i = 0; i < vec1.size(); i++) {
		for (int j = 0; j < vec1[i].val.size(); j++) {
			cout << vec1[i].val[j] << "  ";
		}
		cout << endl;
	}
	cout << query.getID() << " value:" << endl;
	for (int i = 0; i < vec2.size(); i++) {
		for (int j = 0; j < vec2[i].val.size(); j++) {
			cout << vec2[i].val[j] << "  ";
		}
		cout << endl;
	}
#endif // _DEBUG_FOR_SZC_
    cout << "join ..." << endl;
    //Join
    if(ID < query.getID()){  //this的名字在前
        size_t k = 0;
        size_t l = 0;
        while(k < vec1.size() && l < vec2.size()){
            if (!compare(vec1[k], vec2[l])) {//A >= B（B <= A）

#ifdef _DEBUG_FOR_SZC_
				cout << "小于等于1 : ";
				for (int t = 0; t < vec1[k].val.size(); t++) {
					cout << vec1[k].val[t] << "  ";
				}
				cout << " --- ";
				for (int t = 0; t < vec2[l].val.size(); t++) {
					cout << vec2[l].val[t] << "  ";
				}
				cout << endl;
#endif // _DEBUG_FOR_SZC_

                if (equal(vec1[k], vec2[l])) {
#ifdef _DEBUG_FOR_SZC_
					cout << "equal : ";
					for (int t = 0; t < vec1[k].val.size(); t++) {
						cout << vec1[k].val[t] << "  ";
					}
					cout << " --- ";
					for (int t = 0; t < vec2[l].val.size(); t++) {
						cout << vec2[l].val[t] << "  ";
					}
					cout << endl;
#endif // _DEBUG_FOR_SZC_
					size_t start = l;//第一个相同的位置
					size_t end = l + 1;//最后一个相同的位置后一个,第一个不同的位置
					for (; end < vec2.size(); end++) {//找到vec2中最后一个相同的
                        if (equal(vec1[k], vec2[end])){
#ifdef _DEBUG_FOR_SZC_
							cout << "equal : ";
							for (int t = 0; t < vec1[k].val.size(); t++) {
								cout << vec1[k].val[t] << "  ";
							}
							cout << " --- ";
							for (int t = 0; t < vec2[end].val.size(); t++) {
								cout << vec2[end].val[t] << "  ";
							}
							cout << endl;
#endif // _DEBUG_FOR_SZC_
                            continue;
                        }
                        else break;
					}
                    for (; start < end; start++){//开始组合
                        vector<size_t> temp_re(vec1[k].val);//A结果列
                        for (size_t p = 0; p < vec2[start].val.size(); p++){
                            size_t y;
							for (y = 0; y < B.size(); y++) {//判断是否是共同变量
                                if (B[y] == p) break;
							}
							if (y == B.size()) {//不是共同变量
                                temp_re.push_back(vec2[start].val[p]);
							}
                        }
                        valRe.push_back(temp_re);//加入到最终结果
                    }
                    k++;
                }else{
                    l++;
                }
            }else{
                k++;
            }
        }
    }else{
        size_t k = 0;
        size_t l = 0;
        while (k < vec1.size() && l < vec2.size()) {
            if (!compare(vec1[k], vec2[l])) {//A >= B

#ifdef _DEBUG_FOR_SZC_
				cout << "小于等于2 : ";
				for (int t = 0; t < vec1[k].val.size(); t++) {
					cout << vec1[k].val[t] << "  ";
				}
				cout << " --- ";
				for (int t = 0; t < vec2[l].val.size(); t++) {
					cout << vec2[l].val[t] << "  ";
				}
				cout << endl;
#endif // _DEBUG_FOR_SZC_

                if (equal(vec1[k], vec2[l])) {//A==B
#ifdef _DEBUG_FOR_SZC_
					cout << "equal : ";
					for (int t = 0; t < vec1[k].val.size(); t++) {
						cout << vec1[k].val[t] << "  ";
					}
					cout << " --- ";
					for (int t = 0; t < vec2[l].val.size(); t++) {
						cout << vec2[l].val[t] << "  ";
					}
					cout << endl;
#endif // _DEBUG_FOR_SZC_
                    size_t start = l;//第一个相同的位置
                    size_t end = l + 1;//最后一个相同的位置后一个,第一个不同的位置
                    for (; end < vec2.size(); end++) {//找到vec2中最后一个相同的
                        if (equal(vec1[k], vec2[end])){
#ifdef _DEBUG_FOR_SZC_
							cout << "equal : ";
							for (int t = 0; t < vec1[k].val.size(); t++) {
								cout << vec1[k].val[t] << "  ";
							}
							cout << " --- ";
							for (int t = 0; t < vec2[end].val.size(); t++) {
								cout << vec2[end].val[t] << "  ";
							}
							cout << endl;
#endif // _DEBUG_FOR_SZC_
                            continue;
                        }
                        else break;
                    }
                    for (; start < end; start++) {//开始组合
                        vector<size_t> temp_re(vec2[k].val);//B结果列
                        for (size_t p = 0; p < vec1[start].val.size(); p++) {//将A结果列加入
                            size_t y;
                            for (y = 0; y < A.size(); y++) {//判断是否是共同变量
                                if(p == A[y]) break;
                            }
                            if(y == A.size()){//不是共同变量
                                temp_re.push_back(vec1[start].val[p]);
                            }
                        }
                        valRe.push_back(temp_re);//加入到最终结果
                    }
                    k++;
                }else{
                    l++;
                }
            }else{
                k++;
            }
        }
    }
    cout << "join end" << endl;
    for(auto a:queryStr){
        queryStrRe.push_back(a);
    }
    for(auto a:queryStrVec){
        queryStrRe.push_back(a);
    }

#ifdef _DEBUG_FOR_SZC_
	cout << "join结果: " << ID << " join " << query.getID() << " 得到 " << id << " :" << endl;
	for (int i = 0; i < valRe.size(); i++) {
		cout << i + 1 << ":  ";
		for (int j = 0; j < valRe[i].size(); j++) {
			cout << valRe[i][j] << "  ";
		}
		cout << endl;
	}
#endif // _DEBUG_FOR_SZC_

    queryClass* re = new queryClass(queryStrRe, nameRe, valRe, id, ID, query.getID(), 2);
    return re;
}

