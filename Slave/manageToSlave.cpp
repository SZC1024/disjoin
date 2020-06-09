//
//  manageToSlave.cpp
//  garphDatabase
//
//  Created by songzc on 2019/12/17.
//  Copyright © 2019 songzc. All rights reserved.
//  节点管理类

#include "manageToSlave.hpp"

manageToSlave::manageToSlave(){
    networkTraffic = 0;
    //读取host配置文件
    ifstream in("./host");
    in>>ID;
    size_t id;
    string line;
    while(in>>id>>line){
        if (id > STORE_START_NUM) STORE_COMPUTE_SPLIT = 1;
        ipref[id] = line;
    }
    
    serverToSlave = new server();   //对slave节点通信
    serverToMaster = new serverSlave();  //对master节点通信
}

//管理
manageToSlave::manageToSlave(size_t id){
    networkTraffic = 0;
    ID = id;
    serverToSlave = new server();   //对slave节点通信
    serverToMaster = new serverSlave();  //对master节点通信
    
}

//添加查询类映射
bool manageToSlave::addQuery(size_t id, unordered_map<size_t, string> sub, unordered_map<size_t, vector<string> > nameUmap){
    
    if(umap_Gen_Query.find(id) != umap_Gen_Query.end()){
        
        cout<<"该查询已经存在"<<endl;
        return false;
    }
    
    //构建全局查询类
    generalQuery* query = new generalQuery(id, sub, nameUmap);
    
    unordered_map<size_t, generalQuery*>::value_type val(id, query);
    umap_Gen_Query.insert(val);
    //query->executeSubQuery();//把这个执行过程单独拎出来作为一个阶段
    return true;
}

//更换slave节点映射
bool manageToSlave::alterIPref(unordered_map<size_t, string> ip){
    
    ip.swap(ipref);
    
    return true;
}

//新增节点映射
bool manageToSlave::addIPref(size_t num, string ip){
    
    unordered_map<size_t, string>::value_type val(num, ip);
    
    ipref.insert(val);
    
    return true;
}

//获取ID
size_t manageToSlave::getID() const{
    
    return ID;
}

//获取IP映射
unordered_map<size_t, string> manageToSlave::getIPref() const{
    
    return ipref;
}

//开始进行监听工作
void manageToSlave::myStartToSlave(){
    
    serverToSlave->mystart();
    pthread_t thcon;
    pthread_t thget;
    cout<<"slave对slave开始监听"<<endl;
    //accept链接进程
    pthread_create(&thcon, nullptr, serverStartConn_Slave, (void*) serverToSlave);
    pthread_detach(thcon);
    
    //收发数据线程
    pthread_create(&thget, nullptr, serverGetData_Slave, (void*) this);
    pthread_detach(thget);
    
    //循环不让线城结束
    while(1){
        usleep(1000000);
    }
}

struct manageSend{
    manageToSlave* managetoslave;
    int it;
    size_t id[2];
    manageSend(manageToSlave* manTS,int i,size_t* j){
        managetoslave = manTS;
        it = i;
        id[0] = j[0];
        id[1] = j[1];
    }
};

bool manageToSlave::threadSendDataToSlave(int it, size_t* id){
	queryClass* temp = getSubQuery(id[0], id[1]);
	if (temp == nullptr || temp->getID() == 0) {
		//cout<<"当前查询ID不存在，ID "<<id[0]<<" "<<id[1]<<endl; 
		return false;
	}
	cout << "收到的ID请求" << id[0] << " " << id[1] << endl;
	size_t id_1 = temp->getID();   //ID
	size_t idL = temp->getParentLeft(); //父左ID
	size_t idR = temp->getpParentRight();  //父右ID
	int type_1 = temp->getType();    //类型
	vector<string> queryStr_1 = temp->getQueryVec();
	vector<string> name_1 = temp->getValNameVec();
	vector<vector<size_t> > val_1 = temp->getValueVec();
	//发送数据
	//格式
	//查询类ID 子查询ID 父左ID 父右ID 类型
	//查询语句个数 查询语句1 查询语句2
	//结果变量个数 变量1 变量2
	//结果条数
	//每条结果有多少个变量
	//结果变量1 结果变量2
	serverToSlave->mySend(it, &id[0], sizeof(size_t));
	serverToSlave->mySend(it, &id_1, sizeof(size_t));
	serverToSlave->mySend(it, &idL, sizeof(size_t));
	serverToSlave->mySend(it, &idR, sizeof(size_t));
	serverToSlave->mySend(it, &type_1, sizeof(int));
	//发送查询语句
	size_t count_qu = queryStr_1.size();
	serverToSlave->mySend(it, &count_qu, sizeof(count_qu));
	for (size_t m = 0; m < count_qu; m++) {
		string str_1 = queryStr_1.at(m);
		serverToSlave->mySend(it, (void*)str_1.c_str(), str_1.size());
	}
	//发送查询变量
	size_t count_val = name_1.size();
	serverToSlave->mySend(it, &count_val, sizeof(size_t));
	for (size_t g = 0; g < count_val; g++) {
		string str_1 = name_1.at(g);
		serverToSlave->mySend(it, (void*)str_1.c_str(), str_1.size());
	}
	size_t tempnetworkTraffic = networkTraffic;
	//发送结果值
	size_t count_value = val_1.size();
	serverToSlave->mySend(it, &count_value, sizeof(count_value));
	for (size_t f = 0; f < count_value; f++) {
		size_t count_d = val_1[f].size();
		serverToSlave->mySend(it, &count_d, sizeof(count_d));
		for (size_t s = 0; s < count_d; s++) {
			size_t re = val_1[f].at(s);
			serverToSlave->mySend(it, &re, sizeof(re));
			networkTraffic += sizeof(re);
		}
	}
	cout << id[0] << " " << id[1] << " 发送完毕" << endl;
	cout << "本次向外发送数据量为" << networkTraffic - tempnetworkTraffic << "字节" << endl;
    return true;
}

//由于pthread不能传递成员函数，故创建此函数
void* sendResult(void* managesend) {
	manageSend* temp = static_cast<manageSend*>(managesend);
	temp->managetoslave->threadSendDataToSlave(temp->it,temp->id);
	return 0;
}

//slave to slave服务器接收数据
bool manageToSlave::getAndSendData_To_Slave(){
    
    cout<<"slave 对 slave 收发数据"<<endl;
	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 500;
    while(1){
        list<int> coList = serverToSlave->getConnList();
        fd_set rfds;
        FD_ZERO(&rfds);
        for(auto it = coList.begin(); it != coList.end(); it++){
            //cout<<"slave Scoket:"<<*it<<endl;
            //检测是否关闭
            FD_SET(*it, &rfds);
        }
        int retval = 0;
        retval = select(1024, &rfds, NULL, nullptr, &tv);
        if(retval == -1){
            cout<<"slave 对 slave select 错误"<<endl;
            FD_ZERO(&rfds);
        }
        else if(retval == 0){
            //cout<<"slave 对slave no message"<<endl;
		    FD_ZERO(&rfds);
        }
        else{  //slave节点通信的服务端,客户端发送格式为ID ID，
            for(auto it = coList.begin(); it != coList.end(); it++){
                if(FD_ISSET(*it, &rfds) == 0){	 
		            continue;
	        	}
                size_t id[2] = {0};
                //cout<<"收到数据请求前"<<endl;
                serverToSlave->myRec(*it, id);
                //cout<<"收到数据请求后"<<endl;
                //此部分根据ID：ID发送数据

                pthread_t threadsend;
                manageSend* managesend = new manageSend(this, *it, id);
				pthread_create(&threadsend, nullptr, sendResult, managesend);
				pthread_detach(threadsend);
                /*
                queryClass* temp = getSubQuery(id[0], id[1]);
                if(temp == nullptr || temp->getID() == 0) {
                    //cout<<"当前查询ID不存在，ID "<<id[0]<<" "<<id[1]<<endl; 
                    continue;
                }
                cout<<"收到的ID请求"<<id[0]<<" "<<id[1]<<endl;
                size_t id_1 = temp->getID();   //ID
                size_t idL = temp->getParentLeft(); //父左ID
                size_t idR = temp->getpParentRight();  //父右ID
                int type_1 = temp->getType();    //类型
                vector<string> queryStr_1 = temp->getQueryVec();
                vector<string> name_1 = temp->getValNameVec();
                vector<vector<size_t> > val_1 = temp->getValueVec();
                
                //发送数据
                //格式
                //查询类ID 子查询ID 父左ID 父右ID 类型
                //查询语句个数 查询语句1 查询语句2
                //结果变量个数 变量1 变量2
                //结果条数
                //每条结果有多少个变量
                //结果变量1 结果变量2
                serverToSlave->mySend(*it, &id[0], sizeof(size_t));                
                serverToSlave->mySend(*it, &id_1, sizeof(size_t));
                serverToSlave->mySend(*it, &idL, sizeof(size_t));
                serverToSlave->mySend(*it, &idR, sizeof(size_t));
                serverToSlave->mySend(*it, &type_1, sizeof(int));
                
                //发送查询语句
                size_t count_qu = queryStr_1.size();
                serverToSlave->mySend(*it, &count_qu, sizeof(count_qu));
                for(size_t m = 0; m < count_qu; m++){
                    string str_1 = queryStr_1.at(m);
                    serverToSlave->mySend(*it, (void*) str_1.c_str(), str_1.size());
                }
                
                //发送查询变量
                size_t count_val = name_1.size();
                serverToSlave->mySend(*it, &count_val, sizeof(size_t));
                for(size_t g = 0; g < count_val; g++){
                    string str_1 = name_1.at(g);
                    serverToSlave->mySend(*it, (void*) str_1.c_str(), str_1.size());
                }
                
                size_t tempnetworkTraffic = networkTraffic;
                //发送结果值
                size_t count_value = val_1.size();
                serverToSlave->mySend(*it, &count_value, sizeof(count_value));
                for(size_t f = 0; f < count_value; f++){
                    size_t count_d = val_1[f].size();
                    serverToSlave->mySend(*it, &count_d, sizeof(count_d));
                    for(size_t s = 0; s<count_d; s++){
                        size_t re = val_1[f].at(s);
                        serverToSlave->mySend(*it, &re, sizeof(re));
                        networkTraffic += sizeof(re);
                    }
                }
                cout << id[0] << " " << id[1] << " 发送完毕" << endl;
                cout << "本次向外发送数据量为" << networkTraffic - tempnetworkTraffic << "字节" << endl;
                */
            }
        FD_ZERO(&rfds);
        }
        usleep(100000);
    }
    return true;
}

//Salve对Master节点服务器收发数据
bool manageToSlave::getAndSendData_To_Master(){
    
    cout<<"slave 对 master开始收发数据"<<endl;
    struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 500;
    while(1){
        fd_set rfds;
        FD_ZERO(&rfds);
        list<int> coList = serverToMaster->getConnList();
        for(auto it = coList.begin(); it != coList.end(); it++){
            //cout<<"socket: "<<*it<<endl;
            //检测是否关闭
            FD_SET(*it, &rfds);
        }
        int retval = 0;
        retval = select(1024, &rfds, NULL, nullptr, &tv);
        if(retval == -1){
            cout<<"select 错误"<<endl;
            FD_ZERO(&rfds);               
        }
        else if(retval == 0){
            //cout<<"no message"<<endl;
            FD_ZERO(&rfds); 
        }
        else{
            for(auto it = coList.begin(); it != coList.end(); it++){
                if(FD_ISSET(*it, &rfds) == 0){
		            continue;
                }
                //Master和Slave节点通信的服务端
                //分为“create”， “close", "sentense", "plan"，“global”
                char buffer[1024];
                memset(buffer, 0, 1024);
                serverToMaster->myRec(*it, buffer);
                string str(buffer);
                cout<<str<<endl;
                if(str == "create"){
                    //加载数据库
                    cout<<"创建数据库"<<endl;
                    size_t createok = 0;
				    if (create("./mydatabase/")) {
                        createok = 1;
					    serverToMaster->mySend(*it, &createok, sizeof(createok));
                    }else {
                        createok = 0;
                        serverToMaster->mySend(*it, &createok, sizeof(createok));
                    }
                }
                else if(str == "sentense"){
                    //发送查询语句
                    //ID
                    //查询语句个数
                    //ID query 名字个数 查询变量名数组
                    size_t queryID;   //总查询ID
                    size_t countQ;   //查询语句个数
                    vector<size_t> subID;  //子查询ID
                    vector<string> subStrV;  //子查询语句
                    vector<vector<string>> nameV; //子查询变量名
                    serverToMaster->myRec(*it, &queryID); //接收查询ID
                    serverToMaster->myRec(*it, &countQ);   //接收子查询ID个数
                    
                    for(size_t j = 0; j < countQ; j++){  //接收查询
                        size_t id3 = 0;   //子查询ID
                        char buf[2048];  //子查询语句
                        size_t count_s = 0;  //子查询变量名个数
                        char bufName[128];  //子查询变量名
                        memset(buf, 0, sizeof(buf));
                        memset(bufName, 0, sizeof(bufName));
                        serverToMaster->myRec(*it, &id3);  //接收子查询ID
                        serverToMaster->myRec(*it, buf);   //接收查询语句
                        string strBuf(buf);
                        serverToMaster->myRec(*it, &count_s);  //接收变量名个数
                        vector<string> nVec;   //变量名数组
                        for(size_t k = 0; k < count_s; k++){  //接收变量名
                            memset(bufName, 0, sizeof(bufName));
                            serverToMaster->myRec(*it, bufName);
                            string str(bufName);
                            nVec.push_back(str);
                        }
                        subID.push_back(id3);
                        subStrV.push_back(strBuf);
                        nameV.push_back(nVec);
                    }
                    
                    //创建查询类
                    unordered_map<size_t, string> sub1;//子查询ID -> 子查询语句
                    unordered_map<size_t, vector<string> > nameU;//子查询ID -> 子查询变量名数组
                    for(size_t l = 0; l < subID.size(); l++){
                        sub1[subID.at(l)] = subStrV.at(l);
                        nameU[subID.at(l)] = nameV.at(l);
                    }

                    //在执行查询之前，考虑到子查询有可能在其他节点执行的快，在本节点执行得慢
                    //当其他节点执行到连接计划步骤时，本节点还在执行查询
                    //那么其他节点会向本节点请求数据，但是此时本节点还没有创建查询类映射
                    //因此查询类映射应该在子查询执行之前就发送到slave节点

                    //创建子查询，但不执行
                    addQuery(queryID, sub1, nameU);
                }
                else if(str == "executeSubQuery"){
                    //只接受一个id，为总查询类id，命令slave执行开始总查询类id为此的所有子查询语句
                    size_t generalQueryId;
                    serverToMaster->myRec(*it, &generalQueryId);
                    if(umap_Gen_Query.find(generalQueryId)==umap_Gen_Query.end()){
                        cout << "该总查询不存在:" << generalQueryId << endl;
                        exit(0);
                    }else{
                        generalQuery* gQuery = umap_Gen_Query[generalQueryId];
                        gQuery->executeSubQuery();
                    }
                    //如果想做到同步，可以在这里之后对master发送完成信号，当master收集到所有slave的完成信号之后再发送连接计划
                    size_t temp = 1;
                    serverToMaster->mySend(*it, &temp, sizeof(size_t));
                }
                else if(str == "closeDB"){
                    //关闭数据库
                    cout<<"关闭数据库"<<endl;
                    closeDB();
                    //myclose();
                }
                else if(str == "close"){
                    cout << "关闭服务器" << endl;
                    myclose();
                }
                else if(str == "plan"){
                    //接收查询计划，格式为
                    //查询ID
                    //行数
                    // ID type
                    size_t id1;   //查询ID
                    size_t countP;  //查询计划个数
                    vector<structPlan> plan1;  //查询计划
                    size_t id_1;
                    int type_1;
                    
                    serverToMaster->myRec(*it, &id1);
                    serverToMaster->myRec(*it, &countP);
                    for(size_t m = 0; m < countP; m++){ //接收查询计划
                        serverToMaster->myRec(*it, &id_1);
                        serverToMaster->myRec(*it, &type_1);
                        structPlan stp;
                        stp.ID = id_1;
                        stp.type = type_1;
                        plan1.push_back(stp);
                    }
                    cout << "接收到的连接计划为:" << endl;
                    for (int i = 0; i < plan1.size();i++) {
                        cout << "plan[" << i << "] = <" << plan1[i].ID << "," << plan1[i].type << ">" << endl;
                    }
                    cout<<"接收连接计划完成，下面开始执行连接计划"<<endl;
                    //传入查询计划并执行
                    if (!plan1.empty()) {
                        cout << "连接计划非空" << endl;
                        generalQuery* temp = umap_Gen_Query[id1];
                        size_t idRe = plan1.at(0).ID;
                        cout << "变更连接计划前" << endl;
                        if (temp == nullptr) cout << "temp是一个空结果: ID :" << id1 << endl;
                        temp->alterPlan(plan1);
                        cout << "连接计划变更完成" << endl;
                    }else cout << "连接计划为空，该节点无需执行" << endl;
                    //else{
                    //    //如果没有查询计划，返回0结果
                    //    cout<<"没有连接计划，返回结果0"<<endl;
                    //    size_t re_1 = 0; //子查询为0
                    //    size_t re_2 = 0;  //返回零结果
                    //    size_t re_3 = 1; //返回一条结果
                    //    size_t re_4 = 1; //结果只有一列
                    //    serverToMaster->mySend(*it, &id1, sizeof(size_t));
                    //    serverToMaster->mySend(*it,&re_1 , sizeof(size_t));
                    //    serverToMaster->mySend(*it,&re_3 , sizeof(size_t));
                    //    serverToMaster->mySend(*it,&re_4 , sizeof(size_t));
                    //    serverToMaster->mySend(*it,&re_2 , sizeof(size_t));
                    //}
                    cout<<"连接计划接收完成"<<endl;
                    //如果想做到同步，可以在这里之后对master发送完成信号，当master收集到所有slave的完成信号之后再发送执行连接计划命令
                    size_t temp = 1;
                    serverToMaster->mySend(*it, &temp, sizeof(size_t));
                }
                else if(str == "executePlan"){
                    size_t generalQueryId;
                    size_t generalRootId;//用于判断该slave的树根是不是全局连接计划树根
                    serverToMaster->myRec(*it, &generalQueryId);
                    serverToMaster->myRec(*it, &generalRootId);
                    if (umap_Gen_Query.find(generalQueryId)==umap_Gen_Query.end()) {
                        cout << "该总查询不存在:" << generalQueryId << endl;
                        exit(0);
                    }else {
                        cout << "开始执行连接计划" << endl;
                        generalQuery* gQuery = umap_Gen_Query[generalQueryId];
                        gQuery->executePlan();
						cout << "连接计划执行完毕" << endl;
                        //cout << "本节点向外发送数据总量为" << networkTraffic << "字节" << endl;
						vector<vector<size_t> > result;
                        size_t idRe = (gQuery->plan)[0].ID;
						queryClass* qc = gQuery->getSubQueryClass(idRe);
						result = qc->getValueVec();
                        /*//发送结果
                        //结果格式：
                        //查询类ID
                        //子查询类ID
                        //结果条数
                        //每条结果列数 变量 变量 变量a
                        cout<<"结果："<<endl;
                        for(size_t it_1 = 0; it_1 < result.size(); it_1++){
                            for(size_t it_2 = 0; it_2 < result[it_1].size(); it_2++)
                                cout<<result[it_1].at(it_2)<<" ";
                            cout<<" "<<endl;
                        }
                        cout<<"结果完成"<<endl;
                        */
						size_t countRe = result.size();
                        if(idRe==generalRootId){
							serverToMaster->mySend(*it, &generalQueryId, sizeof(size_t));
							serverToMaster->mySend(*it, &idRe, sizeof(size_t));
							serverToMaster->mySend(*it, &countRe, sizeof(size_t));
							for (size_t n = 0; n < result.size(); n++) {
								size_t num = result.at(n).size();
								serverToMaster->mySend(*it, &num, sizeof(size_t));
								for (size_t g = 0; g < num; g++) {
									size_t num1 = result[n].at(g);
									serverToMaster->mySend(*it, &num1, sizeof(size_t));
								}
							}
                        }else{
                            size_t temp = 0;
                            serverToMaster->mySend(*it, &temp, sizeof(size_t));
                        }
                    }
				}
                else if(str == "global"){
                    //传入全局ID映射
                    //ID
                    //个数
                    //ID node
                    size_t count_g;
                    size_t id_5;
                    serverToMaster->myRec(*it, &id_5);
                    serverToMaster->myRec(*it, &count_g);
                    unordered_map<size_t, size_t> unM;
                    size_t node[2] = {0};
                    for(size_t h = 0; h < count_g; h++){
                        serverToMaster->myRec(*it, node);
                        unM[node[0]] = node[1];
                    }
                    generalQuery* temp_g = umap_Gen_Query[id_5];
                    temp_g->alterSubQueryID(unM);
                    cout<<"全局ID映射接收完成"<<endl;
                }
                else{
                    cout<<"slave节点接收Master节点数据出错:"<< str<<endl;
                    close(*it);
                    serverToMaster->ereaConnList(*it);
                }
            }
        FD_ZERO(&rfds);
        }
        usleep(100000);
    }
    return true;
}
//对MAster节点的服务器开启监听
void manageToSlave::myStartToMaster(){
    
    serverToMaster->mystart();
    
    cout<<"对master节点服务器开启"<<endl;
    pthread_t thcon;
    pthread_t thget;
    //accept链接进程
    pthread_create(&thcon, nullptr, serverStartConn_Master, (void*)serverToMaster);
    pthread_detach(thcon);
    
    //收发数据线程
    pthread_create(&thget, nullptr, serverGetData_Master, (void*)this);
    pthread_detach(thget);
    while(1){
        usleep(1000000);
    }
}

bool manageToSlave::removeQuery(size_t id){
    
    unordered_map<size_t, generalQuery* >::iterator it;
    
    it = umap_Gen_Query.find(id);
    if(it == umap_Gen_Query.end()){
        cout<<"要删除的查询语句不存在"<<endl;
        return false;
    }
    else{
        umap_Gen_Query.erase(id);
    }
    return true;
}

//判断是否有查询
bool manageToSlave::is_Empty_Query() const{
    
    if(umap_Gen_Query.empty()){
        return true;
    }
    return false;
}

//判断slave对slave服务器是否存在
bool manageToSlave::is_NULL_ServerToSlave() const{
    
    if(serverToSlave == nullptr){
        return true;
    }
    return false;
}

//判断slave对master服务器是否存在
bool manageToSlave::is_NULL_ServerToMaster() const{
    
    if(serverToMaster == nullptr){
        return true;
    }
    return false;
}

manageToSlave::~manageToSlave(){
    
    if(serverToMaster != nullptr)
        delete serverToMaster;
    if(serverToSlave != nullptr)
        delete serverToSlave;
    serverToMaster = nullptr;
    serverToSlave = nullptr;
}

//得到子查询，id1是总查询id，id2是子查询id
queryClass* manageToSlave::getSubQuery(size_t id1, size_t id2){
    
    unordered_map<size_t, generalQuery* >::iterator it;
    it = umap_Gen_Query.find(id1);
    if(it == umap_Gen_Query.end()){
        //cout<<"请求的查询不存在"<<endl;
        //exit(0);
    }
    else{
        generalQuery* temp= umap_Gen_Query[id1];
        queryClass* temp1 =  temp->getSubQueryClass(id2);
        if(temp1->getID() == 0){
            size_t nodeNum = temp->getNode(id2);
            if(nodeNum == ID){//属于该节点但是还未完成，等待
                while(1){
                    queryClass* temp2 = temp->getSubQueryClass(id2);
                    if (temp2->getID() == 0) {
                        delete temp2;
                        //cout << "总查询" << id1 << "中的子查询" << id2 << "属于该节点但还未完成，等待1s" << endl;
                    }
                    else {
                        return temp2;
                    }
                    usleep(1000000);
                }
            }
            else{
                cout<<"该查询语句不属于这个节点"<<endl;
                return temp1;
            }
        }
        else {
           return temp1;
        }
    }
    return nullptr;
}

//关闭服务器
bool manageToSlave::myclose(){
    
    serverToMaster->myclose();
    serverToSlave->myclose();
    if(serverToSlave != nullptr){
        delete serverToSlave;
        serverToSlave = nullptr;
    }
    if(serverToMaster != nullptr){
        delete serverToMaster;
        serverToMaster = nullptr;
    }
    return true;
}

//由于pthread不能传递成员函数，故创建此函数
//对slave服务器开始工作
void* serverStart_Slave(void* ser) {
    static_cast<manageToSlave*>(ser)->myStartToSlave();
    return 0;
}

//对master的服务器开始工作
void* serverStart_Master(void* ser) {
  static_cast<manageToSlave*>(ser)->myStartToMaster();
    return 0;
}


//开始收发数据，对Master
void* serverGetData_Master(void* serv) {
    static_cast<manageToSlave*>(serv)->getAndSendData_To_Master();
    return 0;
}

//开始收发数据,对Slave
void* serverGetData_Slave(void* serv) {
    static_cast<manageToSlave *>(serv)->getAndSendData_To_Slave();
    return 0;
}
