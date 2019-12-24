//
//  generalQuery.cpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//

#include "generalQuery.hpp"

generalQuery::generalQuery(){
    
    ID = 0;
    MaxSubID = 1;  //初始化ID值
}

//根据ID和str创建查询
generalQuery::generalQuery(size_t id, string str){
    
    ID = id;
    queryStr =str;
    MaxSubID = 1;
    
    //生成ip映射和客户端映射
    ifstream in("./host");
    
    if(! in){
        cout<<"Master节点打开配置文件失败"<<endl;
        exit(0);
    }
    
    size_t id1 ;
    size_t id2;
    string str1;
    in>>id1;
    while(in>>id2>>str1){
        
        if(id1 == id2) continue;
        else{
            ipRef[id2] = str1;
            clRef[id2] = new client(str1, PORT);
	    clRef[id2]->createSocket();
	    clRef[id2]->myConnect();
        }
    }
}

//查询分解，需赋值查询语句和查询语句变量名
bool generalQuery:: decomposeQueryAll(){ //赋值子查询语句
    
    if(! queryComposeToVec(queryStr.c_str())){
        cout<<"分解查询失败"<<endl;
        return false;
    }
    return true;
}

//从子查询文件中读取
bool generalQuery:: readAllQuery(string& fileName){    //fileName为查询文件绝对路径，例如/home/test，不指定具体文件格式,返回查询文件名

   vector<string> result;   //子查询语句string
   string Dir = "./subQuery/";   //Master节点的子查询文件固定保存在subQuery下面
   string file = (Dir + fileName);  //file保存文件的绝对路径
   ifstream in(file.c_str());    //创建读文件对象
   string temp;
   string Str = "";
   if(!in.is_open()){
     cout<<"打开文件失败"<<endl;
     return false;
   }
   //赋值最终变量名
    getline(in, temp);
    size_t start1 = 0;
    size_t end1 = 0;
    vector<string> vec1;
    for(end1 = 0; end1 < temp.size(); end1++){
        if(temp.at(end1) == ' '){
            string str(temp.begin() + start1, temp.begin() + end1);
            start1 = end1 + 1;
            vec1.push_back(str);
        }
    }
    if(temp.at(temp.size() - 1) != ' '){
        string str(temp.begin() + start1, temp.begin() + end1);
        vec1.push_back(str);
    }
    //输出调试
    cout<<"查询语句变量："<<endl;
    for(auto it = vec1.begin(); it != vec1.end(); it++){
        cout<<*it<<" ";
    }
    cout<<" "<<endl;
    finalResultName.swap(vec1);
    
    //赋值查询语句变量名
    while(getline(in, temp)){
       if(temp == "---") break;
       //赋值变量名数组
       size_t start = 0;
       size_t end = 0;
       vector<string> vec;
       for(end = 0; end < temp.size(); end++){
           if(temp.at(end) == ' '){
               string str(temp.begin() + start, temp.begin() + end);
               start = end + 1;
               vec.push_back(str);
           }
       }
        
       if(temp.at(temp.size() - 1) != ' '){
           string str(temp.begin() + start, temp.begin() + end);
           vec.push_back(str);
       }
        
       //输出调试
        cout<<"查询语句变量1111111111："<<vec.size()<<endl;
       for(auto it = vec.begin(); it != vec.end(); it++){
           cout<<*it<<" ";
       }
       cout<<" "<<endl;
       
       //赋值
       subStrValName.push_back(vec);
   }
    cout<<"变量名个数: "<<subStrValName.size()<<endl;
   while(getline(in, temp)){
      if(temp == "---"){       //关闭旧写入对象，创建新写入对象
         // string str1 = Str;
         string str1 = transformQuery(Str);
         result.push_back(str1);
         cout<<"子查询语句: "<<str1<<endl;
         Str = "";
      }
      else{                       //写入到对应文件对象
        Str = Str + temp + "\n";
      }
  }
    //赋值查询语句
    subStr.swap(result);
    return true;
}

//查询文件分解
string generalQuery::queryDecomposeFile(string& queryFile){       //queryFile为查询文件名，无后缀，例如lubm2,结果保存在。/subquery的目录下的queryFile文件中

  string Dir = "./Query/";
  string subDir = "./subQuery/";
  string result;
 // string cmdString = "java -jar ./dis-triplebitCHN-1.0-SNAPSHOT-jar-with-dependencies.jar";
  string cmdString = "java -jar ./dis-triplebit-1.0-SNAPSHOT-jar-with-dependencies.jar";
  cmdString += " ";
  cmdString += Dir;
  cmdString += queryFile;
  cmdString += " ";
  cmdString += subDir;
  cmdString += queryFile;

  int pid;
  pid = system(cmdString.c_str());

 if(pid == 0){
    cout<<"查询语句分解成功"<<endl;
    cout<<"查询分解命令为："<<cmdString<<endl;
    result = queryFile;
  }
 else{
     cout<<"查询语句分解失败"<<endl;
     cout<<"查询分解命令为："<<cmdString<<endl;
     result = " ";
  }
  return result;
}

//将查询语句分解成子查询语句数组
bool generalQuery::queryComposeToVec(const char* querySen){

 cout<<"查询里面："<<querySen<<endl;
 vector<string> resultVec;   //结果数组
 string queryFile = "queryC";     //查询语句名，默认queryC
 string Dir = "./Query/";
 string temp = to_string(ID);
 queryFile += temp;

 temp = Dir + queryFile;   //将查询语句string保存成查询文件
 ofstream out(temp.c_str());  //创建写入文件流out
 out<<querySen<<endl;   //将查询语句输入
 out.close();
    
 string queryName = queryDecomposeFile(queryFile); //查询文件的分解
 readAllQuery(queryName);     //得到子查询语句数组
 return true;
}



//创建partition,
bool generalQuery:: createParition(){
    cout<<"创建分区："<<"子查询语句有："<<subStr.size()<<endl;
    for(size_t i = 1; i < ipRef.size() + 1; i++){   //根据节点个数创建分区
        
        unordered_map<size_t, string> umap;
        unordered_map<size_t, size_t> umap_re;
        unordered_map<size_t, vector<string> > umap_name;
        for(size_t j = 0; j < subStr.size(); j++){
            
            //调用接口得到该查询语句大概有多少条
            size_t temp = 0;
           // temp = 1000;
	   cout<<"getRsultSzie: "<<endl;
	  
            temp = 1000;
            if(temp == 0){
                continue;
            }
            else{
                umap[MaxSubID] = subStr.at(j);
                umap_re[MaxSubID] = temp;
                umap_name[MaxSubID] = subStrValName.at(j);
                MaxSubID++;
		cout<<"当前IDa："<<MaxSubID<<endl;
            }
        }
        partSub[i] = new partitionToSub(i, umap, umap_re, umap_name);
    }
    
    return true;
}

 //创建查询计划
bool generalQuery::createPlan(){
    
    cout<<"创建查询计划"<<endl;
    //得到分区对应的子查询类的集合
    //先生成所有union的子查询类（在生成的过程构建总查询树）
    //再生成所有join的子查询类（在生成的过程构建总查询树）
    //（另外在调用join与union函数的时候需要传参subquery的id，这个id在本类中以MaxSubID参数实现）
    //总查询树构建结束之后进行分解（根据subquery的类型（type参数））

    for(auto i:subStr){
        cout<<"subStr:"<<i<<endl;
    }


    map<size_t,subQuery*> idtosubq;//第一个参数为ID
    for(auto pts:partSub){
        vector<size_t> subid = pts.second->getAllSubID();
        for(int i=0;i<subid.size();i++){
            idtosubq[subid[i]]=pts.second->getSubQuery(subid[i]);
        }
    }

    for(auto i:idtosubq){
        cout<<"初始idtosubq:"<<i.first<<"\t"<<i.second->getID()<<"\t"<<i.second->getType()<<endl;
    }

    cout<<"断点1"<<endl;
    map<size_t,int> flag;//第一个参数表示subQuery的ID，第二个参数表示该subQuery是否被union（1）以及join（2）过，从未是0
    map<size_t,size_t> tree;//第一个参数表示树中的结点id，第二个参数表示该节点的父节点，根节点的父节点为0
    for(map<size_t,subQuery*>::iterator iter=idtosubq.begin();iter!=idtosubq.end();iter++) flag[iter->first]=0;

    for(auto i:flag){
        cout<<"初始flag:"<<i.first<<"\t"<<i.second<<endl;
    }

    cout<<"断点3"<<endl;
    //此时idtosubq内存储subquery的指针
    for(map<size_t,subQuery*>::iterator i=idtosubq.begin();i!=idtosubq.end();i++){//生成所有union
	    cout<<"外层循环i:"<<i->first<<endl;
        if(flag[i->first]==0){//说明左孩子可以unoin
            //开始寻找右孩子
            map<size_t, subQuery*>::iterator j = i;
            j++;
            for(;j!=idtosubq.end();j++){
		        cout<<"内层循环j:"<<j->first<<endl;		
                if(flag[j->first]==0){//说明右孩子可以union
                    //生成所有的union类型子查询类
                    if(i->second->isCommon(*(j->second)) == 1){//这里需要知道返回值的意义
                        cout<<"内层if:"<<i->second->isCommon(*(j->second))<<"\t MaxSubID:"<<MaxSubID<<endl;
                        idtosubq[MaxSubID]=i->second->Union(*(j->second),MaxSubID);
                        flag[i->first]=1;
                        flag[j->first]=1;
                        flag[MaxSubID]=0;
                        tree[MaxSubID]=0;
                        tree[i->first]=MaxSubID;
                        tree[j->first]=MaxSubID;
                        MaxSubID++;
			            break;
                    }
                }
            }
        }
    }
    cout<<"断点2"<<endl;
    //union结束，开始join，原理同上
    for(map<size_t,subQuery*>::iterator i=idtosubq.begin();i!=idtosubq.end();i++){
        cout<<"循环2外层循环i:"<<i->first<<endl;

        if(flag[i->first]==0){//说明左孩子可以join
            map<size_t, subQuery*>::iterator j = i;
            j++;
            for(;j!=idtosubq.end();j++){
                cout<<"循环2内层循环j:"<<j->first<<endl;
                if(flag[j->first]==0){//说明右孩子可以join
                    vector<string> temp;
                    if(i->second->findCommonValName(*(j->second),temp)){//这里需要知道返回值的意义
                        cout<<"内层if:"<<i->second->findCommonValName(*(j->second),temp)<<"\t MaxSubID:"<<MaxSubID<<endl;
                        idtosubq[MaxSubID]=i->second->Join(*(j->second),MaxSubID);
                        flag[i->first]=2;
                        flag[j->first]=2;
                        flag[MaxSubID]=0;
                        tree[MaxSubID]=0;
                        tree[i->first]=MaxSubID;
                        tree[j->first]=MaxSubID;
                        MaxSubID++;
                    }
                }
            }
        }
    }
    //join结束之后只剩最大id的flag的value对应的是0
    //接下来该利用tree生成查询计划树
    cout<<"分解查询计划前"<<endl;
    for(auto i:tree){
        cout<<"tree:"<<i.first<<"\t"<<i.second<<endl;
    }
    for(auto i:idtosubq){
        cout<<"idtosubq:"<<i.first<<"\t"<<i.second->getID()<<"\t"<<i.second->getType()<<endl;
    }
    PlanTree* generalPlanTree = new PlanTree(&tree, idtosubq);
    cout<<"1"<<endl;
    vector<PlanTree*>* planTreeForEachPartition = nullptr;
    cout<<"2"<<endl;
    decomposePlan(generalPlanTree, planTreeForEachPartition, partSub.size());
    cout<<"分解查询计划以后"<<endl;
    return true;
}

void printTree(TreeNode* node){
    if(node!=nullptr){
        cout<<node->id<<"\t"<<node->type<<endl;
        printTree(node->left);
        printTree(node->right);
    }else{
        cout<<"null"<<endl;
    }
}

//分解查询计划，直接将查询计划复制到分区的子查询计划中
bool generalQuery::decomposePlan(PlanTree* generalPlanTree,vector<PlanTree*>* planTreeForEachPartition,size_t num){
    cout<<"进入查询计划"<<endl;
    
    printTree(generalPlanTree->root);//这里的输出显示建树成功，plantree的结构的对的
    cout<<"↖plantree↗"<<endl;

    planTreeForEachPartition = generalPlanTree->decomposePlanTree(num);

    for(int i=0;i<planTreeForEachPartition->size();i++){
        printTree(planTreeForEachPartition->at(i)->root);
        cout<<"↖i="<<i<<"↗"<<endl;

    }

    vector<vector<structPlan>*>* partitionPlan=new vector<vector<structPlan>*>();//里边存了每个partition的structPlan数组，用来发给每个partition
    for (size_t i = 0; i < planTreeForEachPartition->size(); i++) {
        //对每一个小PlanTree都执行，1：计划树转为完全二叉树，2：计划树转为vector存储，3：vector的元素转为structPlan结构
        PlanTree* each = planTreeForEachPartition->at(i);
        each->toCompleteBinaryTree(each->root);

        printTree(each->root);//输出显示这里的完全二叉树结构也是正确的
        cout<<"↖complete tree i="<<i<<"↗"<<endl;

        vector<TreeNode*>* eachTreeNodeVector = each->completeBinaryTreeToVector(each->root);

        for(int ii=0;ii<eachTreeNodeVector->size();ii++){
            cout<<"tree node vector "<<ii<<"\t"<<eachTreeNodeVector->at(ii)->id<<"\t"<<eachTreeNodeVector->at(ii)->type<<endl;
        }//输出显示这里的vector结构正确，是完全二叉树的数组存储法

        vector<structPlan>* eachstructPlanVector=new vector<structPlan>();//这里不应该用栈变量，不然的话跳出语法块就被销毁了
        for (size_t j = 0; j < eachTreeNodeVector->size(); j++) {
            //将TreeNode结构的vector转换为structPlan结构的vector
            structPlan* temp = new structPlan();
            temp->ID = eachTreeNodeVector->at(j)->id;//虚节点id为0
            if (eachTreeNodeVector->at(j)->type==-1) {
                temp->type = 0;
            }else {
                temp->type = eachTreeNodeVector->at(j)->type;
            }
            eachstructPlanVector->push_back(*temp);
        }
        
        for(int ii=0;ii<eachstructPlanVector->size();ii++){
            cout<<"eachstructPlanVector "<<ii<<":"<<eachstructPlanVector->at(ii).ID<<"\t"<<eachstructPlanVector->at(ii).type<<endl;
        }//这里输出说明structPlanVector的结构是正确的

        //TreeNode vector到structPlan vector转换结束。为eachstructPlanVector
        //接下来将eachstructPlanVector复制到每个partition的子查询计划中
        partitionPlan->push_back(eachstructPlanVector);
    }

    //暂时只将总查询计划下发
    size_t selected = 2;
    partSub[selected]->alterSubPlan(*(partitionPlan->at(0)));//这里的alterSubPlan的实现用了swap函数，所以导致我在后边查partitionPlan查不出东西
    for (unordered_map<size_t, partitionToSub*>::iterator iter = partSub.begin(); iter != partSub.end();iter++) {
        if (iter->first!=selected) {
            vector<structPlan> temp;
            structPlan a;
            a.ID = 0;
            a.type = 0;
            temp.push_back(a);
            partSub[iter->first]->alterSubPlan(temp);
        }
    }

    //通过注释上边包含alterSubPlan函数的代码块，这里的输出显示出传给alterSubPlan的vector结构正确
    for(int i=0;i<partitionPlan->size();i++){
        cout<<"i="<<i<<endl;
        for(int j=0;j<partitionPlan->at(i)->size();j++){
            cout<<partitionPlan->at(i)->at(j).ID<<"\t";
        }
        cout<<endl;
        for(int j=0;j<partitionPlan->at(i)->size();j++){
            cout<<partitionPlan->at(i)->at(j).type<<"\t";
        }
        cout<<endl;
    }


    return true;
}


//发送查询计划,发送各分区子查询计划到对应节点a
//先发送查询ID
//先发送结构体个数
//再发送结构体 ID type
//可以考虑多线程做
bool generalQuery::sendPlan(){
    cout<<"发送查询计划"<<endl;
    for(size_t i = 1; i < partSub.size() + 1; i++){
        partitionToSub* temp = partSub[i];
        size_t count = temp->getSubPlanSize();
        vector<structPlan> temp2 = temp->getSubPlan();
        client* cl = clRef[i];
        cl->mySend((void*)"plan", 5);
	size_t id_10 = ID;
	cl->mySend(&id_10, sizeof(size_t));
        cl->mySend(&count, sizeof(size_t));
        for(size_t j = 0; j < temp2.size(); j++){
            size_t id = temp2.at(j).ID;
            int type1 = temp2.at(j).type;
            cl->mySend(&id, sizeof(size_t));
            cl->mySend(&type1, sizeof(int));
        }
    }
    return true;
}

//开始执行查询
bool generalQuery::mystart(){
    
    //查询分解
    if(! decomposeQueryAll()){
        cout<<"查询分解失败"<<endl;
        return false;
    }
    
    //创建分区
    if(!createParition()){
        cout<<"创建分区失败"<<endl;
        return false;
    }
    
    //发送子查询语句
    if(! sendSubqueryToSlave()){
        cout<<"发送子查询语句失败"<<endl;
        return false;
    }
    
    //创建查询计划
    if(! createPlan()){
        cout<<"创建查询计划失败"<<endl;
        return false;
    }
    
    //创建全局查询ID
    if(!createGlobalRef()){
        cout<<"创建全局查询ID失败"<<endl;
    }
    
    //发送全局ID
    if(!sendGlobalRef()){
        cout<<"发送全局ID失败"<<endl;
        return false;
    }
    
    //发送查询计划，此处需要等到结果发送过来以后才能结束
    if(!sendPlan()){
        
        cout<<"发送全局查询计划失败"<<endl;
    }
    
    return true;
}

//返回查询结果
vector<vector<size_t>> generalQuery::getResult(){
    
    return result;
}

//先发送查询类ID
//在发送子查询个数
//发送子查询语句,发送格式 ID 查询语句 变量名数组个数 变量名数组
/*
 4   总变量个数，变量名数组个数不算进去
 1 uy 2 ui ui
 */
bool generalQuery::sendSubqueryToSlave(){
    
    cout<<"开始发送子查询"<<endl;
    vector<size_t> eCount;  //总变量计数
    for(size_t i = 1; i < partSub.size() + 1; i++){ //计算子查询个数,分区从1开始
        size_t count = 0;
        partitionToSub* temp = partSub[i];
        vector<size_t> id = temp->getAllSubID();
        count = id.size();
	cout<<"分区id个数1： "<<count<<endl;
        eCount.push_back(count);  //从0开始存！！
    }
    
    //此处可以考虑多线程,发送查询语句
    for(size_t i = 1; i < partSub.size() + 1; i++){  //根据分区发送
        partitionToSub* temp = partSub[i];
        vector<size_t> id = temp->getAllSubID();
	cout<<"分区id个数: "<<id.size()<<endl;
        client* cl = clRef[i];
        size_t count = eCount.at(i - 1);   //！！差值
        size_t id3 = ID;
	cout<<"查询ID： "<<ID<<endl;
        cl->mySend((void *)"sentense", 9);  //先发送信号
        cl->mySend(&id3, sizeof(size_t)); //发送查询类ID
        cl->mySend(&count, sizeof(size_t)); //发送总个数
        for(size_t j = 0; j < id.size(); j++){ //发送子查询
            
            vector<string> queryStr1 = temp->getSubQueryStr(id.at(j));
            vector<string> queryName = temp->getSubQueryName(id.at(j));
            
            //发送ID
            size_t id2 = id.at(j);
            cl->mySend(&id2, sizeof(size_t));
            
            //发送查询语句
            cl->mySend((void*) queryStr1.at(0).c_str(), queryStr1.at(0).size());
            cout<<"查询语句"<<queryStr1.at(0)<<endl;
             //发送变量名数组个数
            size_t nameNum = queryName.size();
            cl->mySend(&nameNum, sizeof(size_t));
            
            //发送变量名
	    cout<<"变量名22:"<<endl;
            for(size_t k = 0; k < queryName.size(); k++){
                string str1 = queryName.at(k);
		cout<<"变量名11："<<str1<<endl;
                cl->mySend((void*)str1.c_str(), str1.size());
            }
        }
    }
   cout<<"结束发送查询语句"<<endl;
    return true;
}

//创建全局映射表
bool generalQuery::createGlobalRef(){
    
    for(auto it = partSub.begin(); it != partSub.end(); it++){
        partitionToSub* temp = it->second;
        vector<size_t> sub = temp->getAllSubID();
        for(auto it_2 = sub.begin(); it_2 != sub.end(); it_2++){
            globalIDRef[*it_2] = it->first;
        }
    }
    return true;
}

//发送全局映射表
//先发送ID
//在发送个数
//再发送ID Node
bool generalQuery::sendGlobalRef(){
    cout<<"发送全局映射表"<<endl;
    size_t id = ID;
    size_t count = globalIDRef.size();
    for(size_t i = 1; i <clRef.size() + 1; i++){
        client* cl = clRef[i];
        cl->mySend((void *)"global", 7);  //发送信号
        cl->mySend(&id, sizeof(size_t)); //发送ID
        cl->mySend(&count, sizeof(size_t)); //发送映射个数
        
        unordered_map<size_t, size_t>::iterator it;
        for(it = globalIDRef.begin(); it != globalIDRef.end(); it++){
            size_t id1 = (*it).first;
            size_t node = it->second;
	    size_t id_5[2];
	    id_5[0] = id1;
	    id_5[1] = node;
            cl->mySend(&id1, sizeof(id_5));
        }
    }
    return true;
}
