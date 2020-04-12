//
//  generalQuery.cpp
//  garphDatabase
//
//  Created by 周华健 on 2019/12/15.
//  Copyright © 2019 周华健. All rights reserved.
//  查询类实现
#include "generalQuery.hpp"

//默认构造函数
generalQuery::generalQuery(){
    ID = 0;
}

//根据查询语句映射和查询语句变量名映射和ID创建查询类
generalQuery::generalQuery(size_t id, unordered_map<size_t, string> sub, unordered_map<size_t, vector<string> > nameUmap){
    
    if(id == 0){
        cout<<"ID 为0的generalQuery不能被构造"<<endl;
        return;
    }
    //if(sub.empty() || nameUmap.empty()){
    //    cout<<"构造查询类时，sub 或 nameUmap出错"<<endl;
    //}
    ID = id;
    subQueryNamevec = nameUmap;
    subQueryStr = sub;    
    //赋值全局ip映射
    size_t id1;
    size_t id2;
    string str;
    ifstream in("./host");
    if(! in){
        cout<<"读取ip映射失败"<<endl;
        exit(0);
    }
    in>>id1;
    myNodeId = id1;
    while(in>>id2>>str){
        if (id2 > STORE_START_NUM) STORE_COMPUTE_SPLIT = 1;
        if (id2 == id1 || id2 == 0) continue;
        else{//创建客户端
            unmap_cl[id2] = new client(str, PORT_SLAVE);
            unmap_cl[id2]->createSocket();
            unmap_cl[id2]->myConnect();
            serverIPRef[id2] =str;
        }
    }
    in.close();
}

bool generalQuery::executeSubQuery(){
    //构造value_type类型
    for(auto it = subQueryStr.begin(); it != subQueryStr.end(); it++){
        vector<string> queryStr;//查询语句vec
        vector<string> name = subQueryNamevec[(*it).first];//得到namevec
        queryStr.push_back((*it).second);
        unordered_map<size_t, queryClass*>::value_type val((*it).first, new queryClass(queryStr, name, (*it).first));//执行
        ownedSubQuery.insert(val);
    }
    return true;
}

//变更全局ID映射，返回-1表示失败，正确赋值表示成功
bool generalQuery::alterSubQueryID(unordered_map<size_t, size_t> umap){
    cout<<"进入变更函数"<<endl;
    if(umap.empty()){
        cout<<"变更全局id映射失败"<<endl;
        return  false;
    }
     cout<<"清0前"<<endl;  
    //非空清0
    if(!globalSubQueryID.empty()){
        globalSubQueryID.clear();
    }
    cout<<"清零后"<<endl;
    //交换
    globalSubQueryID.swap(umap);

    cout << "全局映射表：<queryID,node> =";
    for(auto a:globalSubQueryID){
		cout << " <" << a.first << "," << a.second << ">";
    }
    cout << endl;
    return true;
}

//新增子查询
size_t generalQuery::addSubQuery(size_t id, queryClass* query){
    
    if(id == 0){
        cout<<"generalQuery::addSubQuery 无法添加 id为0 的子查询类"<<endl;
    }
    
    if(query == nullptr){
        cout<<"generalQuery::addSubQuery 无法添加空子查询类"<<endl;
    }
    
    unordered_map<size_t, queryClass*>::value_type val(id, query);
    ownedSubQuery.insert(val);
    return id;
}

//获取查询类ID
size_t generalQuery::getID() const{
    
    return ID;
}

//获取节点, -1代表不存在
size_t generalQuery::getNode(size_t id) const{
    
    size_t temp = -1;
    auto it = globalSubQueryID.find(id);
    if(it!= globalSubQueryID.end()){
        temp = it->second;
    }
    else{
        cout<<"此id不存在globalSubQueryId中："<< id <<endl;
    }
    
    return temp;
}

//获取子查询对象
queryClass*  generalQuery::getSubQueryClass(size_t id){
    
    auto it = ownedSubQuery.find(id);
    if(it != ownedSubQuery.end()){
        return it->second;
    }
    else{
        cout<<"当前获取不到此id对应的子查询"<<endl;
        queryClass* errorRe = new queryClass();
        return errorRe;
    }
}

//获取变量名
vector<string> generalQuery::getName(size_t id) const{
    
    auto it = subQueryNamevec.find(id);
    if(it != subQueryNamevec.end()){
        return it->second;
    }
    else{
        cout<<"generalQuery::getName无法获取不存在的idy映射。"<<endl;
        vector<string> errorRe;
        return errorRe;
    }
}

//变更查询计划
bool generalQuery::alterPlan(vector<structPlan> plan1){
    
    plan1.swap(plan);
    return true;
}

//执行查询计划
bool generalQuery::executePlan(){
     
    if(plan.empty()){
        cout<<"查询计划为空"<<endl;
        return false;
    }
    cout<<"循环执行查询计划"<<endl;
    recycleEx(0);
    return true;
}

//循环执行查询计划
bool generalQuery::recycleEx(size_t index){
    //我的循环执行版本
    for (index = plan.size() - 1; index >= 0; index--) {
        if (plan[index].ID == 0) continue;
        if (globalSubQueryID[plan[index].ID] == myNodeId){
            if (ownedSubQuery.find(plan[index].ID) == ownedSubQuery.end() && plan[index].type == 0) {
                while(1){
                    queryClass* temp=getSubQueryClass(plan[index].ID);
                    if (temp->getID() != 0) break;
                    usleep(1000000);
                }
            }
            if (plan[index].type == 1) ownedSubQuery[plan[index].ID] = ownedSubQuery[plan[index * 2 + 1].ID]->Union(*(ownedSubQuery[plan[index * 2 + 2].ID]), plan[index].ID);
            if (plan[index].type == 2) ownedSubQuery[plan[index].ID] = ownedSubQuery[plan[index * 2 + 1].ID]->Join(*(ownedSubQuery[plan[index * 2 + 2].ID]), plan[index].ID);
        }else{
            auto it_node = globalSubQueryID.find(plan[index].ID);
            if (it_node == globalSubQueryID.end()) {
                cout<< "该子查询不在全局映射表中：" << plan[index].ID << endl;
                exit(0);
            }
            size_t nodenum = it_node->second;
            size_t idV[2];
            idV[0] = ID;
            idV[1] = plan[index].ID;
			auto it_client = unmap_cl.find(nodenum);
			if (it_client == unmap_cl.end()) {
				cout << "节点的客户端映射不存在：" << nodenum << endl;
				exit(0);
			}
			client* cl = it_client->second;
			cout << "没有的ID：" << idV[1] << " 向" << nodenum << "发送请求" << endl;
			cl->mySend(idV, sizeof(idV));//发送格式ID ID，前者为查询类ID，后者为子查询ID
			cout << "发送请求结束" << endl;
			size_t quID;  //查询ID
			size_t subID; //子查询ID
			size_t id_l;   //左ID
			size_t id_r;    //右ID
			int type_1;    //类型
			vector<string> strVec;  //子查询语句
			vector<string> name_1;   //查询变量名
			vector<vector<size_t> > val_1;  //查询语句值
			cout << "接收数据 " << idV[1] << endl;
			cl->myRec(&quID);
			cl->myRec(&subID);
			cl->myRec(&id_l);
			cl->myRec(&id_r);
			cl->myRec(&type_1);
			cout << "查询ID：" << quID << endl;
			cout << "子查询ID: " << subID << endl;
			cout << "左查询ID：" << id_l << endl;
			cout << "右查询ID：" << id_r << endl;
			cout << "类型：" << type_1 << endl;
			size_t count_qu;
			cl->myRec(&count_qu);
			cout << "查询语句个数:" << count_qu << endl;
			for (size_t m = 0; m < count_qu; m++) {
				char A[2048];
				memset(A, 0, 2048);
				cl->myRec(A);
				string str_1(A);
				strVec.push_back(str_1);
			}
			size_t count_va;
			cl->myRec(&count_va);
			for (size_t n = 0; n < count_va; n++) {
				char A[128];
				memset(A, 0, 128);
				cl->myRec(A);
				string str_1(A);
				name_1.push_back(str_1);
			}
			size_t count_value;
			cl->myRec(&count_value);
			for (size_t l = 0; l < count_value; l++) {
				size_t count_2;
				vector<size_t> re_1;
				cl->myRec(&count_2);
				for (size_t p = 0; p < count_2; p++) {
					size_t va;
					cl->myRec(&va);
					re_1.push_back(va);
				}
				val_1.push_back(re_1);
			}
			ownedSubQuery[subID] = new queryClass(strVec, name_1, val_1, subID, id_l, id_r, type_1);
			if (ownedSubQuery[subID] != nullptr) cout << "得到：" << subID << endl;
			else cout << "得到：" << subID << "失败" << endl;
        }
    }

    /*
    //周华健的递归执行版本
    if(index >= plan.size() || plan.at(index).ID == 0) return true;
    else{
        recycleEx(index * 2 + 1);
        recycleEx(index * 2 + 2);
        
        //获取数据
        if(index * 2 + 1 < plan.size() && ownedSubQuery.find(plan.at(index * 2 + 1).ID) == ownedSubQuery.end() && plan.at(index * 2 + 1).ID != 0){
            
            auto it_node = globalSubQueryID.find(plan.at(index * 2 + 1).ID);
            if(it_node == globalSubQueryID.end()){
                cout<<"该子查询不在全局映射表中："<<plan.at(index * 2 + 1).ID<<endl;
                exit(0);
            }
            size_t nodenum = it_node->second;
            
            size_t idV[2];
            idV[0] = ID;
            idV[1] = plan.at(index * 2 + 1).ID;
            
            auto it_client = unmap_cl.find(nodenum);
            if(it_client == unmap_cl.end()){
                cout<<"节点的客户端映射不存在："<<nodenum<<endl;
                exit(0);
            }
            client* cl = it_client->second;
            cout << "没有的ID：" << idV[1] << " 向" << nodenum << "发送请求" << endl;
            //发送请求
            cl->mySend(idV, sizeof(idV));//发送格式ID ID，前者为查询类ID，后者为子查询ID
            cout << "发送请求结束" << endl;
            //接收数据变量
            size_t quID;  //查询ID
            size_t subID; //子查询ID
            size_t id_l;   //左ID
            size_t id_r;    //右ID
            int type_1;    //类型
            vector<string> strVec;  //子查询语句
            vector<string> name_1;   //查询变量名
            vector<vector<size_t> > val_1;  //查询语句值

            cout << "接收数据 " << idV[1] << endl;
            //接收数据
            cl->myRec(&quID);
            cl->myRec(&subID);
            cl->myRec(&id_l);
            cl->myRec(&id_r);
            cl->myRec(&type_1);
            
            //输出调试
            cout<<"查询ID："<< quID <<endl;
            cout<<"子查询ID: "<<subID<<endl;
            cout<<"左查询ID："<<id_l<<endl;
            cout<<"右查询ID："<<id_r<<endl;
            cout<<"类型："<< type_1 <<endl;
            
            //接受查询语句
            size_t count_qu;
            cl->myRec(&count_qu);
            cout<<"查询语句个数:"<<count_qu<<endl;
            for(size_t m = 0; m < count_qu; m++){
                char A[2048];
                memset(A, 0, 2048);
                cl->myRec(A);
                string str_1(A);
                strVec.push_back(str_1);
            }
            
            //接收查询变量名
            size_t count_va;
            cl->myRec(&count_va);
            for(size_t n = 0; n < count_va; n++){
                char A[128];
                memset(A, 0, 128);
                cl->myRec(A);
                string str_1(A);
                name_1.push_back(str_1);
            }
            
            //接收结果
            size_t count_value;
            cl->myRec(&count_value);
            for(size_t l = 0; l < count_value; l++){
                size_t count_2;
                vector<size_t> re_1;
                cl->myRec(&count_2);
                for(size_t p = 0; p < count_2; p++){
                    size_t va;
                    cl->myRec(&va);
                    re_1.push_back(va);
                }
                val_1.push_back(re_1);
            }
            
            ownedSubQuery[subID] = new queryClass(strVec, name_1, val_1, subID, id_l, id_r, type_1);
            
            if(ownedSubQuery[subID] != nullptr){
                cout<<"得到："<<subID<<endl;
            }
            else{
                cout<<"得到："<<subID<<"失败"<<endl;
            }
        }
        
        //获取数据
        if(index * 2 + 2 < plan.size() &&  ownedSubQuery.find(plan.at(index * 2 + 2).ID) == ownedSubQuery.end()  && plan.at(index * 2 + 2).ID != 0 ){
            
            auto it_node = globalSubQueryID.find(plan.at(index * 2 + 2).ID);
            if(it_node == globalSubQueryID.end()){
                cout<<"该子查询不存在全局映射表"<<plan.at(index * 2 + 2).ID<<endl;
                exit(0);
            }//从slave的报错和master的全局映射表来看，似乎是没有将后来生成的子查询放入到全局映射表中
            size_t nodenum = it_node->second;
            
            size_t idV[2];
            idV[0] = ID;
            idV[1] = plan.at(index * 2 + 2).ID;
 
            auto it_client = unmap_cl.find(nodenum);

            //cout << "节点客户端列表: ";
            //for(auto a:unmap_cl){
            //    cout << a.first << " ";
            //}
            //cout << endl;

            if(it_client == unmap_cl.end()){
                cout<<"节点的客户端映射不存在:"<<nodenum<<endl;
                exit(0);
            }
            client* cl = it_client->second;
            
            cout<<"没有的ID："<<idV[1]<<" 向"<<nodenum<<"发送请求"<<endl;
            
            //发送请求
            cl->mySend(idV, sizeof(idV));//发送格式ID ID，前者为查询类ID，后者为子查询ID
            cout << "发送请求结束" << endl;
            //接收数据变量
            size_t quID = 0;  //查询ID
            size_t subID = 0; //子查询ID
            size_t id_l = 0;   //左ID
            size_t id_r = 0;    //右ID
            int type_1 = 0;    //类型
            vector<string> strVec;  //子查询语句
            vector<string> name_1;   //查询变量名
            vector<vector<size_t> > val_1;  //查询语句值
            
            //接收数据
            cout << "接收数据 " << idV[1] << endl;
            cl->myRec(&quID);
            cl->myRec(&subID);
            cl->myRec(&id_l);
            cl->myRec(&id_r);
            cl->myRec(&type_1);
            
            //接受查询语句
            size_t count_qu;
            cl->myRec(&count_qu);
            cout << "查询语句个数:" << count_qu << endl;
            for(size_t m = 0; m < count_qu; m++){
                char A[2048];
                memset(A, 0, 2048);
                cl->myRec(A);
                string str_1(A);
                strVec.push_back(str_1);
            }
            
            //接收查询变量名
            size_t count_va;
            cl->myRec(&count_va);
            for(size_t n = 0; n < count_va; n++){
                char A[128];
                memset(A, 0, 128);
                cl->myRec(A);
                string str_1(A);
                name_1.push_back(str_1);
            }
            
            //接收结果
            size_t count_value;
            cl->myRec(&count_value);
            for(size_t l = 0; l < count_value; l++){
                size_t count_2;
                vector<size_t> re_1;
                cl->myRec(&count_2);
                for(size_t p = 0; p < count_2; p++){
                    size_t va;
                    cl->myRec(&va);
                    re_1.push_back(va);
                }
                val_1.push_back(re_1);
            }
            ownedSubQuery[subID] =  new queryClass(strVec, name_1, val_1, subID, id_l, id_r, type_1);
            
            if(ownedSubQuery[subID] != nullptr){
                cout<<"得到："<<subID<<endl;
            }
            else{
                cout<<"得到："<<subID<<"失败"<<endl;
            }
        }
        
        //结束
        if(index * 2 + 2 >=  plan.size() || plan.at(index * 2 + 2).ID == 0 || plan.at(index * 2 + 1).ID == 0 || ownedSubQuery.find(plan.at(index * 2 + 2).ID) == ownedSubQuery.end() || ownedSubQuery.find(plan.at(index * 2 + 1).ID) == ownedSubQuery.end()) return false;

        //合并操作
        if(plan.at(index).type == 1 && plan.at(index).ID != 0){
            ownedSubQuery[plan.at(index).ID] = ownedSubQuery[plan.at(index * 2 + 1).ID]-> Union(*(ownedSubQuery[plan.at(index * 2 + 2).ID]), plan.at(index).ID);

			vector<string> A = ownedSubQuery[plan.at(index * 2 + 1).ID]->getValNameVec();
			vector<string> B = ownedSubQuery[plan.at(index * 2 + 2).ID]->getValNameVec();
			vector<string> C = ownedSubQuery[plan.at(index).ID]->getValNameVec();
			cout << "union : < ";
			for (auto a : A) {
				cout << a << " ";
			}
			cout << ">  < ";
			for (auto b : B) {
				cout << b << " ";
			}
			cout << ">  --  < ";
			for (auto c : C) {
				cout << c << " ";
			}
			cout << ">" << endl;

            if(ownedSubQuery[plan.at(index).ID] != nullptr){
                cout<<"生成："<<plan.at(index).ID<<"成功"<<endl;
            }
            else{
                cout<<"生成："<<plan.at(index).ID<<"失败"<<endl;
            }
        }//join操作
        else if(plan.at(index).type == 2 && plan.at(index).ID != 0){
            ownedSubQuery[plan.at(index).ID] = ownedSubQuery[plan.at(index * 2 + 1).ID]->Join(*(ownedSubQuery[plan.at(index * 2 + 2).ID]), plan.at(index).ID);

            vector<string> A = ownedSubQuery[plan.at(index * 2 + 1).ID]->getValNameVec();
            vector<string> B = ownedSubQuery[plan.at(index * 2 + 2).ID]->getValNameVec();
            vector<string> C = ownedSubQuery[plan.at(index).ID]->getValNameVec();
            cout << "join : < ";
            for(auto a:A){
                cout << a << " ";
            }
            cout << ">  < ";
            for(auto b:B){
                cout << b << " ";
            }
            cout << ">  --  < ";
            for(auto c:C){
                cout << c << " ";
            }
            cout << ">" << endl;

			if (ownedSubQuery[plan.at(index).ID] != nullptr) {
				cout << "生成：" << plan.at(index).ID << "成功" << endl;
			}
			else {
				cout << "生成：" << plan.at(index).ID << "失败" << endl;
			}
        }
    }
    */
    return true;
}
