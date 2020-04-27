#include "generalQuery.hpp"

const int _debug_for_szc_ = 1 ;

double get_wall_time() {
	struct timeval time;
	if (gettimeofday(&time, NULL)) return 0;
	return (double)time.tv_sec + (double)time.tv_usec * .0000001;
}

generalQuery::generalQuery(){
    
    ID = 0;
    MaxSubID = 1;  //初始化ID值
}

//根据ID和str创建查询
generalQuery::generalQuery(size_t id, string str){
    cout<<"查询类ID："<<id<<endl;
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
        if (id2 > STORE_START_NUM) STORE_COMPUTE_SPLIT = 1;//该值仅在此有唯一写入
        if (id1 == id2) continue;
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
    
    if(! queryComposeToVec(queryStr.c_str())){//此时的queryStr是具体的总查询语句
        cout<<"分解查询失败"<<endl;
        return false;
    }
    return true;
}

//从子查询文件中读取
bool generalQuery:: readAllQuery(string& fileName){    //fileName为查询文件绝对路径，例如/home/test，不指定具体文件格式,返回查询文件名
    vector<string> result;//子查询语句string
    string Dir = "./subQuery/";//Master节点的子查询文件固定保存在subQuery下面
    string file = (Dir + fileName);//file保存文件的绝对路径
    ifstream in(file.c_str());//创建读文件对象
    string temp;
    string Str = "";
    if(!in.is_open()){
		cout << "打开文件失败" << endl;
		return false;
    }
    //赋值最终变量名
	getline(in, temp);
	size_t start1 = 0;
    size_t end1 = 0;
    vector<string> vec1;
    for(end1 = 0; end1 < temp.size(); end1++){
		if (temp.at(end1) == ' ') {
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
    cout<<"总查询语句变量："<<endl;
    for(auto it = vec1.begin(); it != vec1.end(); it++){
        cout<<*it<<" ";
    }
    cout << endl;
    finalResultName.swap(vec1);
    
    //赋值查询语句变量名
    while(getline(in, temp)){
        if(temp == "---") break;
        //赋值变量名数组
        size_t start = 0;
        size_t end = 0;
        vector<string> vec;
        for(end = 0; end < temp.size(); end++){
			if (temp.at(end) == ' ') {
				string str(temp.begin() + start, temp.begin() + end);//str存一个变量
				start = end + 1;
				vec.push_back(str);
			}
        }//此时vec中存储了一个子查询的所有变量

        if(temp.at(temp.size() - 1) != ' '){
            cout << "进入了不该进入的地方" << endl;
			string str(temp.begin() + start, temp.begin() + end);
			vec.push_back(str);
        }
        
        //输出调试
        cout<<"子查询语句变量："<<vec.size()<<endl;
        for(auto it = vec.begin(); it != vec.end(); it++){
            cout<<*it<<" ";
        }
        cout << endl;

        //赋值
        subStrValName.push_back(vec);
    }
    for(auto i:subStrValName){//调试一下
        cout << "subStrValName:";
        for(auto j:i){
            cout << j << " ";
        }
        cout << endl;
    }
    cout<<"子查询个数: "<<subStrValName.size()<<endl;
    while(getline(in, temp)){
        if(temp == "---"){       //关闭旧写入对象，创建新写入对象
            // string str1 = Str;
            string str1 = transformQuery(Str);
            result.push_back(str1);
            cout<<"子查询语句: "<<str1<<endl;
            Str = "";
        }else{                       //写入到对应文件对象
            Str = Str + temp + "\n";
        }
    }
    //赋值查询语句
    subStr.swap(result);
    return true;
}

//查询文件分解
string generalQuery::queryDecomposeFile(string& queryFile){//queryFile为查询文件名(queryForDecompose)，无后缀，例如lubm2,结果保存在/subquery的目录下
    string Dir = "./Query/";
    string subDir = "./subQuery/";
    string result;
    string cmdString = "java -jar ./dis-triplebit-1.0-SNAPSHOT-jar-with-dependencies.jar";
    cmdString += " ";
    cmdString += Dir;
    cmdString += queryFile;
    cmdString += " ";
    cmdString += subDir;
    cmdString += queryFile;

    int pid = 0;
    pid = system(cmdString.c_str());//暂时使用自己预先写好的子查询，跳过查询分解

    if(pid == 0){
        cout<<"查询语句分解成功"<<endl;
        cout<<"查询分解命令为："<<cmdString<<endl;
        result = queryFile;
    }else{
		cout << "查询语句分解失败" << endl;
		cout << "查询分解命令为：" << cmdString << endl;
		result = " ";
    }
    return result;
}

//将查询语句分解成子查询语句数组
bool generalQuery::queryComposeToVec(const char* querySen){
    cout<<"查询里面："<<querySen<<endl;
    vector<string> resultVec;//结果数组
    string queryFile = "queryForDecompose";//将之前指定的查询文件内容复制一份，放在这个文件里，专门供给查询分解程序使用
    string Dir = "./Query/";
    string temp = to_string(ID);
    queryFile += temp;

    temp = Dir + queryFile;//将查询语句string保存成查询文件(queryForDecompose)
    ofstream out(temp.c_str());//创建写入文件流out
    out<<querySen<<endl;//将查询语句输入
    out.close();
    
    string queryName = queryDecomposeFile(queryFile);//查询文件的分解
    readAllQuery(queryName);//得到子查询语句数组
    return true;
}



//创建partition,
bool generalQuery:: createParition(){
    cout << "创建分区：" << "子查询语句有：" << subStr.size() << endl;
    cout << "ipRef.size() = " << ipRef.size() << endl;
    for(auto ip:ipRef){   //根据节点个数创建分区
        //size_t i = 1; i < ipRef.size() + 1; i++
        unordered_map<size_t, string> umap;
        unordered_map<size_t, size_t> umap_re;
        unordered_map<size_t, vector<string> > umap_name;
        for(size_t j = 0; j < subStr.size(); j++){
            //调用接口得到该查询语句大概有多少条
            size_t temp = 0;
            temp = 1000;
            //cout<<"getRsultSzie: "<<endl;
            //temp = getResultSize(subStr.at(j), i);
            if(temp == 0){
                continue;
            }
            else{
                if(STORE_COMPUTE_SPLIT){
					if (ip.first > STORE_START_NUM) {
						umap[MaxSubID] = subStr.at(j);
						umap_re[MaxSubID] = temp;
						umap_name[MaxSubID] = subStrValName.at(j);
						cout << "当前MaxSubID：" << MaxSubID << endl;
						MaxSubID++;
					}
                }else{
					umap[MaxSubID] = subStr.at(j);
					umap_re[MaxSubID] = temp;
					umap_name[MaxSubID] = subStrValName.at(j);
					cout << "当前MaxSubID：" << MaxSubID << endl;
					MaxSubID++;
                }
            }
        }
        partSub[ip.first] = new partitionToSub(ip.first, umap, umap_re, umap_name);
    }
    return true;
}

//对join图递归地搜非相邻边，dfs pointToEdgeAndType，并得到最多非相邻边集
void dfsThePointToEdgeAndType(map<size_t,map<size_t,int> >& pointToEdgeAndType,map<size_t,size_t>& adjacentEdgeSet,int deep=0){
    int cancontinuesearch = 0;
    //基本情况也包含在下边的判断中（最小适应到两个顶点的基本图）
    //if(_debug_for_szc_){
    //    cout <<"↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓"<< endl;
    //    cout << "deep=" << deep << " 当前join图(pointToEdgeAndType) = point : <edge,selected>" << endl;
    //    for(auto i:pointToEdgeAndType){
    //        cout << i.first << " :";
    //        for(auto j:i.second){
    //            cout << " <" << j.first << "," << j.second << ">";
    //        }
    //        cout << endl;
    //    }
    //}
    for (map<size_t, map<size_t, int> >::iterator point = pointToEdgeAndType.begin(); point != pointToEdgeAndType.end();point++) {
        for (map<size_t, int>::iterator edge = point->second.begin(); edge != point->second.end();edge++) {
            int flag = 0;
            //接下来选一个当前探索的图中没有被选过的独立边
            if(edge->second == 0){
                //该边没有被选中过
                //接下来判断该边的所有邻边有没有被选中过
                for (map<size_t, int>::iterator e = point->second.begin(); e != point->second.end(); e++) {
                    if(e->first != edge->first){//屏蔽掉自身
                        if (e->second == 1) {
                            flag = 1;//但凡有一个是使用过的，flag置为1
                            break;
                        }
                    }
                }
                for (map<size_t, int>::iterator e = pointToEdgeAndType[edge->first].begin(); e != pointToEdgeAndType[edge->first].end(); e++) {
                    if(e->first != point->first){//同样屏蔽掉自身，因为是无向图
                        if (e->second == 1) {
                            flag = 1;//同上
                            break;
                        }
                    }
                }
            }else continue;
			if (flag == 0) {
                //cout << "探索得到当前join图中新独立边: <" << point->first << "," << edge->first << ">" << endl;
                //说明该边的所有邻边也都没有被选中过
                //接下来选中该边并递归
                edge->second = 1;
                pointToEdgeAndType[edge->first][point->first] = 1;//因为是无向图
                cancontinuesearch = 1;
				dfsThePointToEdgeAndType(pointToEdgeAndType, adjacentEdgeSet, deep + 1);
                //cout<<"达到了一次dfs分支末端，撤销 <" << point->first << "," << edge->first << ">，探索其他dfs分支" << endl;
				edge->second = 0;
                pointToEdgeAndType[edge->first][point->first] = 0;
            }
        }
    }
    if(cancontinuesearch==0){
        //说明本次递归一个可选择的边都没有，达到了一个dfs分支的末端
        if (deep > adjacentEdgeSet.size()){//deep也就代表了当前dfs分支末端所选中的非相邻边的数量
            //记录该dfs路径，作为当前寻找到的最多非相邻边组合
            adjacentEdgeSet.clear();
            for (map<size_t, map<size_t, int> >::iterator point = pointToEdgeAndType.begin(); point != pointToEdgeAndType.end();point++) {
                for (map<size_t, int>::iterator edge = point->second.begin(); edge != point->second.end();edge++) {
                    if(edge->second == 1){
                        if((adjacentEdgeSet.find(point->first) == adjacentEdgeSet.end())&&(adjacentEdgeSet.find(edge->first) == adjacentEdgeSet.end())){
                            //因为是无向图，所以当边的两顶点都不包含在adjacentEdgeSet的key时才插入
                            adjacentEdgeSet[point->first] = edge->first;
                        }
                    }
                }
            }
            //if(_debug_for_szc_){
            //    cout << "当前dfs末端deep大于adjacentEdgeSet.size()，更新adjacentEdgeSet:" << endl;
            //    cout << "新的adjacentEdgeSet:";
            //    for (map<size_t, size_t>::iterator i = adjacentEdgeSet.begin(); i != adjacentEdgeSet.end();i++) {
            //        cout << " <" << i->first << "," << i->second << ">";
            //    }
            //    cout << endl;
            //}
        }
    }
}

//从join图中搜索最多的非相邻边集合并返回其中一个组合
map<size_t,size_t>* searchMostAdjacentEdge(map<size_t,set<size_t>* >& pointToEdge){
    //为了模块化，需要根据pointToEdge建立一个搜索专用的join图
    map<size_t, map<size_t, int> > pointToEdgeAndType;//第一个参数和pointToEdge一样，第二个参数map的第一个参数是入边点的id，第二个表示该边有没有被选中过，没有为0，有为1
    for (map<size_t, set<size_t>* >::iterator point = pointToEdge.begin(); point != pointToEdge.end();point++) {
        for (set<size_t>::iterator edge = point->second->begin(); edge != point->second->end();edge++) {
            pointToEdgeAndType[point->first][*edge] = 0;
        }
    }
    //进一个递归函数，对pointToEdgeAndType进行dfs，并得到最多非相邻边集
    map<size_t, size_t>* adjacentEdgeSet = new map<size_t, size_t>();
    /*
	if (_debug_for_szc_) {
		for (auto i : pointToEdgeAndType) {
            cout << "pointToEdgeAndType point:" << i.first << " EdgeAndType:";
			for (auto j : i.second) {
                cout << " <" << j.first << "," << j.second << ">";
			}
            cout << endl;
		}
	}*/
    dfsThePointToEdgeAndType(pointToEdgeAndType,*adjacentEdgeSet);
    return adjacentEdgeSet;
}


 //创建查询计划
bool generalQuery::createPlan(){
    
    cout<<"创建查询计划"<<endl;
    //得到分区对应的子查询类的集合
    //先生成所有union的子查询类（在生成的过程构建总查询树）
    //再生成所有join的子查询类（在生成的过程构建总查询树）
    //（另外在调用join与union函数的时候需要传参subquery的id，这个id在本类中以MaxSubID参数实现）
    //总查询树构建结束之后进行分解（根据subquery的类型（type参数））

    if(_debug_for_szc_){
        for(auto i:subStr){
            cout<<"subStr:"<<i<<endl;
        }
    }

    //将所有分区内的所有子查询汇集起来，存储在idtosubq里边
    //map<size_t,subQuery*> idtosubq;//第一个参数为ID//因为其他函数也需要，所以声明到了类里
    for(auto pts:partSub){
        vector<size_t> subid = pts.second->getAllSubID();//如果是计算与分离架构，在计算节点这里get应该get到一个空vector
        for(int i=0;i<subid.size();i++){
            idtosubq[subid[i]]=pts.second->getSubQuery(subid[i]);
        }
    }

    if(_debug_for_szc_){
        for(auto i:idtosubq){
        cout<<"初始idtosubq:"<<i.first<<"\t"<<i.second->getType()<<endl;
        }
    }

    map<size_t,int> flag;//第一个参数表示subQuery的ID，第二个参数表示该subQuery是否被union（1）以及join（2）过，从未是0
    map<size_t,size_t> tree;//第一个参数表示树中的结点id，第二个参数表示该节点的父节点，根节点的父节点为0
    for(map<size_t,subQuery*>::iterator iter=idtosubq.begin();iter!=idtosubq.end();iter++) flag[iter->first]=0;

    if(_debug_for_szc_){
        for(auto i:flag){
            cout<<"初始flag:"<<i.first<<"\t"<<i.second<<endl;
        }
    }

    cout << "开始union" << endl;
    //此时idtosubq内存储subquery的指针
    for(map<size_t,subQuery*>::iterator i=idtosubq.begin();i!=idtosubq.end();i++){//生成所有union

    //if(_debug_for_szc_) cout<<"外层循环i:"<<i->first<<endl;

        if(flag[i->first]==0){//说明左孩子可以unoin
            //开始寻找右孩子
            map<size_t, subQuery*>::iterator j = i;
            j++;
            for(;j!=idtosubq.end();j++){

                //if(_debug_for_szc_) cout<<"内层循环j:"<<j->first<<endl;

                if(flag[j->first]==0){//说明右孩子可以union
                    //生成所有的union类型子查询类
                    if(i->second->isCommon(*(j->second)) == 1){//这里需要知道返回值的意义
                        //if(_debug_for_szc_) cout<<"内层if:"<<i->second->isCommon(*(j->second))<<"\t MaxSubID:"<<MaxSubID<<endl;
                        idtosubq[MaxSubID]=i->second->Union(*(j->second),MaxSubID);
                        flag[i->first]=1;
                        flag[j->first]=1;
                        flag[MaxSubID]=0;
                        tree[MaxSubID]=0;
                        tree[i->first]=MaxSubID;
                        tree[j->first]=MaxSubID;

                        if(_debug_for_szc_){
							vector<string> A = idtosubq[i->first]->getValNameVec();
							vector<string> B = idtosubq[j->first]->getValNameVec();
							vector<string> C = idtosubq[MaxSubID]->getValNameVec();
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
                        }

                        MaxSubID++;
			            break;//内层循环通过这里之后让他直接结束，因为j必定被使用过了，不需要坐后边的判断
                    }
                }
            }
        }
    }
    cout << "union结束" << endl;

	if (_debug_for_szc_) {
        cout << "union之后,join之前idtosubq,tree,flag信息:" << endl;
        cout << "idtosubq:<id,subquery-type> =";
		for (map<size_t, subQuery*>::iterator i = idtosubq.begin(); i != idtosubq.end(); i++) {
            cout << " <" << i->first << "," << i->second->getType() << ">";
		}
        cout << endl;
        cout << "tree:<id,parent-id> =";
        for (map<size_t, size_t>::iterator i = tree.begin(); i != tree.end();i++) {
            cout << " <" << i->first << "," << i->second << ">";
        }
        cout << endl;
        cout << "flag:<id,used-flag> =";
        for (map<size_t, int>::iterator i = flag.begin(); i != flag.end();i++) {
            cout << " <" << i->first << "," << i->second << ">";
        }
        cout << endl;
	}
    cout << "开始建立join图" << endl;
    //首先建立可join图
    map<size_t, set<size_t>* > pointToEdge;//join图，第一个参数是图中节点id，第二个参数是该节点出边的edge集合，参数为进入顶点的id
    for (map<size_t, subQuery*>::iterator i = idtosubq.begin(); i != idtosubq.end();i++) {
        if(flag[i->first]==0){
            //此时说明i为可使用的节点
            pointToEdge[i->first] = new set<size_t>();
        }
    }
    cout << "join图建点完成，开始建边" << endl;
    //建点完成，接下来建边
    for (map<size_t, set<size_t>* >::iterator i = pointToEdge.begin(); i != pointToEdge.end();i++) {
        map<size_t, set<size_t>* >::iterator j = i;
        j++;
        for (; j != pointToEdge.end();j++) {
            vector<string> temp;
            if(idtosubq[i->first]->findCommonValName(*idtosubq[j->first],temp)){
                //说明i和j可以进行join，于是建边
                i->second->insert(j->first);
                j->second->insert(i->first);
            }
        }
    }
    //建边结束，此时pointToEdge中存的就是完整的子查询连接图（可join图）
    if(_debug_for_szc_){
        cout << "union之后，join之前时，join图信息：" << endl;
        for (map<size_t, set<size_t>* >::iterator i = pointToEdge.begin(); i != pointToEdge.end();i++) {
            cout << "pointToEdge中point:" << i->first << "的相接边:";
            for (set<size_t>::iterator j = i->second->begin(); j != i->second->end();j++) {
                cout << *j << "\t";
            }
            cout << endl;
        }
    }
    cout << "join图创建完成，开始根据join图，合并最多非相邻边，同时join" << endl;
    //接下来就是循环调用searchMostAdjacentEdge函数搜出当前join图中的最多非相邻边并合并
    while(pointToEdge.size() > 1){
        cout << "into searchMostAdjacentEdge" << endl;
        map<size_t, size_t>* adjacentEdgeSet = searchMostAdjacentEdge(pointToEdge);
        cout << "当前join图选中的最多非相邻边组合为:";
        for (auto i = adjacentEdgeSet->begin(); i != adjacentEdgeSet->end();i++) {
            cout << " <" << i->first << "," << i->second << ">";
        }
        cout << endl;
        cout << "out searchMostAdjacentEdge" << endl;
        for (map<size_t, size_t>::iterator pair = adjacentEdgeSet->begin(); pair != adjacentEdgeSet->end();pair++) {
            //对每个点对进行处理，分别在tree中join和join图中合并点
            idtosubq[MaxSubID] = idtosubq[pair->first]->Join(*idtosubq[pair->second], MaxSubID);
            tree[MaxSubID] = 0;
            tree[pair->first] = MaxSubID;
            tree[pair->second] = MaxSubID;
            flag[MaxSubID] = 0;
            flag[pair->first] = 2;
            flag[pair->second] = 2;

            if(_debug_for_szc_){
				vector<string> A = idtosubq[pair->first]->getValNameVec();
				vector<string> B = idtosubq[pair->second]->getValNameVec();
				vector<string> C = idtosubq[MaxSubID]->getValNameVec();
				cout << "join : < ";
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
            }

            //MaxSubID++;
            //接下来对join图进行处理
            pointToEdge[MaxSubID] = new set<size_t>();
            for (set<size_t>::iterator point = pointToEdge[pair->first]->begin(); point != pointToEdge[pair->first]->end(); point++) {
                if(*point != pair->second){//不包含pair中的另一个点
                    pointToEdge[MaxSubID]->insert(*point);
                    pointToEdge[*point]->insert(MaxSubID);
                }
            }
            for (set<size_t>::iterator point = pointToEdge[pair->second]->begin(); point != pointToEdge[pair->second]->end();point++) {
                if(*point != pair->first){//同上，不包含pair中的另一个点
                    pointToEdge[MaxSubID]->insert(*point);
                    pointToEdge[*point]->insert(MaxSubID);
                }
            }
            //加点和边完成，接下来需要将pair中的两个点和所有关联边删掉
            for (set<size_t>::iterator point = pointToEdge[pair->first]->begin(); point != pointToEdge[pair->first]->end();point++) {
                if(*point != pair->second){
                    pointToEdge[*point]->erase(pair->first);
                }
            }
			for (set<size_t>::iterator point = pointToEdge[pair->second]->begin(); point != pointToEdge[pair->second]->end(); point++) {
				if (*point != pair->first) {
					pointToEdge[*point]->erase(pair->second);
				}
			}
            pointToEdge[pair->first]->clear();
            pointToEdge[pair->second]->clear();
            pointToEdge.erase(pair->first);
            pointToEdge.erase(pair->second);
            //对join图合并两个节点完成
            MaxSubID++;
        }
		if (_debug_for_szc_) {
			cout << "以上非相邻边合并之后idtosubq,tree,flag信息:" << endl;
			cout << "idtosubq:<id,subquery-type> =";
			for (map<size_t, subQuery*>::iterator i = idtosubq.begin(); i != idtosubq.end(); i++) {
				cout << " <" << i->first << "," << i->second->getType() << ">";
			}
			cout << endl;
			cout << "tree:<id,parent-id> =";
			for (map<size_t, size_t>::iterator i = tree.begin(); i != tree.end(); i++) {
				cout << " <" << i->first << "," << i->second << ">";
			}
			cout << endl;
			cout << "flag:<id,used-flag> =";
			for (map<size_t, int>::iterator i = flag.begin(); i != flag.end(); i++) {
				cout << " <" << i->first << "," << i->second << ">";
			}
			cout << endl;
		}
    }
    //运行完成后join图只剩一个节点，并且这个节点就是总连接计划树的root，并且已经添加进tree中
    cout << "join结束" << endl;
    //join结束之后只剩最大id的flag的value对应的是0
    //接下来该利用tree生成查询计划树

	if (_debug_for_szc_) {
		cout << "join之后idtosubq,tree,flag信息:" << endl;
		cout << "idtosubq:<id,subquery-type> =";
		for (map<size_t, subQuery*>::iterator i = idtosubq.begin(); i != idtosubq.end(); i++) {
			cout << " <" << i->first << "," << i->second->getType() << ">";
		}
		cout << endl;
		cout << "tree:<id,parent-id> =";
		for (map<size_t, size_t>::iterator i = tree.begin(); i != tree.end(); i++) {
			cout << " <" << i->first << "," << i->second << ">";
		}
		cout << endl;
		cout << "flag:<id,used-flag> =";
		for (map<size_t, int>::iterator i = flag.begin(); i != flag.end(); i++) {
			cout << " <" << i->first << "," << i->second << ">";
		}
		cout << endl;
	}

if(_debug_for_szc_) cout<<"开始创建PlanTree"<<endl;

    PlanTree* generalPlanTree = new PlanTree(&tree, idtosubq);
if(_debug_for_szc_) cout<<"创建PlanTree结束"<<endl;


if(_debug_for_szc_) cout<<"开始进入分解计划"<<endl;
if(_debug_for_szc_) cout << "partSub.size() = " << partSub.size() << endl;
    int computeNodeNum = 0;
    if(STORE_COMPUTE_SPLIT){
		for (auto pts : partSub) {
			if (pts.first < STORE_START_NUM) computeNodeNum++;
		}
    }else{
        computeNodeNum = partSub.size();
    }
    decomposePlan(generalPlanTree, computeNodeNum);
    //decomposePlan(generalPlanTree,2);
if(_debug_for_szc_) cout<<"分解查询计划结束，并将各partition的执行计划已下发"<<endl;
    
    return true;
}


void printTree(TreeNode* node,int firstCall=1){
    if (firstCall == 1) cout << "前序遍历二叉树 <id,type> :";
    if(node!=nullptr){
        cout << " <" << node->id << "," << node->type << ">";
        printTree(node->left, 0);
        printTree(node->right, 0);
    }else{
        cout << " <null>";
    }
    if (firstCall == 1) cout << endl;
}

//分解查询计划，直接将查询计划复制到分区的子查询计划中（被createPlan在结尾调用了）
bool generalQuery::decomposePlan(PlanTree* generalPlanTree,size_t num){

if(_debug_for_szc_) cout<<"进入分解计划"<<endl;
    
    printTree(generalPlanTree->root);//这里的输出显示建树成功，plantree的结构的对的

if(_debug_for_szc_) cout<<"↖分解之前plantree↗"<<endl;
if(_debug_for_szc_) cout<<"开始正式分解"<<endl;

    vector<PlanTree*>* planTreeForEachPartition = generalPlanTree->decomposePlanTree(num);

if(_debug_for_szc_) cout<<"分解正式结束"<<endl;
if(_debug_for_szc_) cout<<"打印出所有分解结果"<<endl;
if(_debug_for_szc_){
    for(int i=0;i<planTreeForEachPartition->size();i++){
        printTree(planTreeForEachPartition->at(i)->root);
        cout<<"↖i="<<i<<"↗"<<endl;
    }
}
if(_debug_for_szc_) cout<<"开始将各个分解之后计划树转换为vector"<<endl;

    vector<vector<structPlan>*>* partitionPlan=new vector<vector<structPlan>*>();//里边存了每个partition的structPlan数组，用来发给每个partition
    for (size_t i = 0; i < planTreeForEachPartition->size(); i++) {
        //对每一个小PlanTree都执行，1：计划树转为完全二叉树，2：计划树转为vector存储，3：vector的元素转为structPlan结构
        PlanTree* each = planTreeForEachPartition->at(i);
        each->toCompleteBinaryTree(each->root);

if(_debug_for_szc_){
    printTree(each->root);//输出显示这里的完全二叉树结构也是正确的
    cout<<"↖complete tree i="<<i<<"↗"<<endl;
}

        vector<TreeNode*>* eachTreeNodeVector = each->completeBinaryTreeToVector(each->root);

if(_debug_for_szc_){
    for(int ii=0;ii<eachTreeNodeVector->size();ii++){
        cout<<"tree node vector "<<ii<<" : "<<eachTreeNodeVector->at(ii)->id<<"    "<<eachTreeNodeVector->at(ii)->type<<endl;
    }//输出显示这里的vector结构正确，是完全二叉树的数组存储法
}

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
        
if(_debug_for_szc_){
    for(int ii=0;ii<eachstructPlanVector->size();ii++){
        cout<<"eachstructPlanVector "<<ii<<" : "<<eachstructPlanVector->at(ii).ID<<"    "<<eachstructPlanVector->at(ii).type<<endl;
    }//这里输出说明structPlanVector的结构是正确的
}

        //TreeNode vector到structPlan vector转换结束。为eachstructPlanVector
        //接下来将eachstructPlanVector复制到每个partition的子查询计划中
        partitionPlan->push_back(eachstructPlanVector);
    }

    if(_debug_for_szc_) cout<<"PlanTree转换vector结束，开始将vector下发给各partition"<<endl;
    
    //partition从1开始递增
    if(_debug_for_szc_){
        for (int i = 0; i < partitionPlan->size();i++) {
            cout << "partitionPlan[" << i << "] (structPlan) <ID,type> :" << endl;
            for (int j = 0; j < partitionPlan->at(i)->size();j++) {
                cout << "<" << partitionPlan->at(i)->at(j).ID << "," << partitionPlan->at(i)->at(j).type << "> ";
            }
            cout << endl;
        }
    }

    //在这里把所有subQuery的查询语句都打印出来看看
    for(auto a:idtosubq){
        cout << "id = " << a.first << " subQuery查询语句:" << endl;
        vector<string> queryvec = a.second->getQueryVec();
        for(auto q:queryvec){
            cout << a.first << " --- " << q << endl;
        }
    }

    //在这里做根据统计索引选择连接计划需要放的位置
    if(STORE_COMPUTE_SPLIT){//计算与存储分离架构不需考虑统计索引
        //在这里做查询计划的下发（这里partitionPlan->size()必定小于等于计算节点的数量（若计算与存储分离架构））
        for (int i = 0; i < partitionPlan->size(); i++) {
            partSub[i + 1]->alterSubPlan(*(partitionPlan->at(i)));
        }
        for(unordered_map<size_t, partitionToSub*>::iterator iter = partSub.begin(); iter != partSub.end(); iter++){
            if(iter->second->getSubPlanSize() == 0){
			    vector<structPlan> temp;
			    structPlan a;
			    a.ID = 0;
			    a.type = 0;
			    temp.push_back(a);
			    partSub[iter->first]->alterSubPlan(temp);
            }
        }
    }else{//混合架构需要考虑（也就是普通模式）
		struct plan_slave_num {
			size_t planid;
			size_t slaveid;
			size_t result;
		};
        vector<plan_slave_num> psn;
        //map<size_t, map<size_t, size_t> > partNumToResult;
        for (size_t partnum = 0; partnum < partitionPlan->size(); partnum++) {
            size_t rootid = partitionPlan->at(partnum)->at(0).ID;
            vector<string> queryStr = idtosubq[rootid]->getQueryVec();
            for (size_t statslaveid = 0; statslaveid < clRef.size(); statslaveid++) {
                size_t result = 0;
                for(auto str:queryStr){
                    result += 1;//getResultSize(str, statslaveid);
                }
                //partNumToResult[partnum][statslaveid + 1] = result;
                plan_slave_num* p = new plan_slave_num;
                p->planid = partnum;
                p->slaveid = statslaveid + 1;//因为getResultSize函数中slave是0开头的，而partSub中slave是1开头的
                p->result = result;
                psn.push_back(*p);
            }
        }//之后partNumToResult中存储了每个partitionPlan中根节点的所有子查询语句（之和）对应每个slave节点的统计索引值
        map<size_t, size_t> plantoslave;
        while(!psn.empty()){
            sort(psn.begin(), psn.end(), [](plan_slave_num a, plan_slave_num b)->bool {return a.result < b.result; });//从大到小排序
            size_t selectedplanid = psn[0].planid;
            size_t selectedslaveid = psn[0].slaveid;
            plantoslave[selectedplanid] = selectedslaveid;
            for (vector<plan_slave_num>::iterator iter = psn.begin(); iter != psn.end();) {
                if(iter->slaveid == selectedslaveid){
                    iter = psn.erase(iter);
                    continue;
                }
                if (iter->planid == selectedplanid) {
                    iter = psn.erase(iter);
                    continue;
                }
                iter++;
            }
        }//在此之后plantoslave中存储了每个plan应该发送给哪个slave节点执行
        for(auto a:plantoslave){
            partSub[a.second]->alterSubPlan(*(partitionPlan->at(a.first)));
        }
		for (unordered_map<size_t, partitionToSub*>::iterator iter = partSub.begin(); iter != partSub.end(); iter++) {
			if (iter->second->getSubPlanSize() == 0) {
				vector<structPlan> temp;
				structPlan a;
				a.ID = 0;
				a.type = 0;
				temp.push_back(a);
				partSub[iter->first]->alterSubPlan(temp);
			}
		}
    }

    //应该在这里也将partitionToSub中的subRef也更新(利用partitionToSub的addSubref函数)
    //因为他涉及到了全局映射表的信息完整
    //插入优先级：最底层查询 > 连接计划根 > 其他所有(一般子查询)
    //最底层查询已经在创建的时候插入过了
    set<size_t> floorSubQuery;
    for(unordered_map<size_t, partitionToSub*>::iterator i = partSub.begin(); i != partSub.end(); i++){
        vector<size_t> temp = i->second->getAllSubID();
        for(auto a:temp){
            floorSubQuery.insert(a);
        }
    }
    set<size_t> rootSubQuery;
    for (unordered_map<size_t, partitionToSub*>::iterator i = partSub.begin(); i != partSub.end();i++) {
        if(i->second->subPlan.at(0).ID != 0){//说明该partition中有实实在在的连接计划
            rootSubQuery.insert(i->second->subPlan.at(0).ID);
            i->second->addSubref(i->second->subPlan.at(0).ID, idtosubq[i->second->subPlan.at(0).ID]);
            cout << "root subid: " << i->second->subPlan.at(0).ID << endl;
        }
    }
    //插入一般子查询
    for (unordered_map<size_t, partitionToSub*>::iterator i = partSub.begin(); i != partSub.end();i++) {
        if(i->second->subPlan.at(0).ID != 0){//说明该partition中有实实在在的连接计划
            for(auto a:i->second->subPlan){
                if(a.ID != 0){//真实子查询
                    set<size_t>::iterator f = floorSubQuery.find(a.ID);
                    if(f == floorSubQuery.end()){
                        set<size_t>::iterator r = rootSubQuery.find(a.ID);
                        if(r == rootSubQuery.end()){
                            //按照插入优先级，此时的子查询(a)属于一般子查询，因此插入
                            i->second->addSubref(a.ID, idtosubq[a.ID]);
                            cout << "other subid: " << a.ID << endl;
                        }
                    }
                }
            }
        }
    }

    //暂时只将总查询计划下发
    //size_t selected = 3;
    //partSub[selected]->alterSubPlan(*(partitionPlan->at(0)));//这里的alterSubPlan的实现用了swap函数，所以导致我在后边查partitionPlan查不出东西
    //for (unordered_map<size_t, partitionToSub*>::iterator iter = partSub.begin(); iter != partSub.end();iter++) {
    //    if (iter->first!=selected) {
    //        vector<structPlan> temp;
    //        structPlan a;
    //        a.ID = 0;
    //        a.type = 0;
    //        temp.push_back(a);
    //        partSub[iter->first]->alterSubPlan(temp);
    //    }
    //}

if(_debug_for_szc_) cout<<"任务下发结束，本模块结束"<<endl;

    //通过注释上边包含alterSubPlan函数的代码块，这里的输出显示出传给alterSubPlan的vector结构正确
    // for(int i=0;i<partitionPlan->size();i++){
    //     cout<<"i="<<i<<endl;
    //     for(int j=0;j<partitionPlan->at(i)->size();j++){
    //         cout<<partitionPlan->at(i)->at(j).ID<<"\t";
    //     }
    //     cout<<endl;
    //     for(int j=0;j<partitionPlan->at(i)->size();j++){
    //         cout<<partitionPlan->at(i)->at(j).type<<"\t";
    //     }
    //     cout<<endl;
    // }

    return true;
}


//发送查询计划,发送各分区子查询计划到对应节点a
//先发送查询ID
//先发送结构体个数
//再发送结构体 ID type
//可以考虑多线程做
bool generalQuery::sendPlan(){
    cout<<"发送连接计划"<<endl;
    for(auto pts:partSub){
        //size_t i = 1; i < partSub.size() + 1; i++
        //auto it_p = partSub.find(i);
        //if(it_p == partSub.end()){
        //    cout<<"不存在分区，分区ID："<<i<<endl;
        //    exit(0);
        //}
        //partitionToSub* temp = it_p->second;
        size_t count = pts.second->getSubPlanSize();
        vector<structPlan> temp2 = pts.second->getSubPlan();
        
        cout<<"连接计划："<< pts.first <<"计划大小 "<< temp2.size() <<endl;
        
		auto it_cl = clRef.find(pts.first);
		if (it_cl == clRef.end()) {
			cout << "不存在客户端:" << pts.first << endl;
			exit(0);
		}
        client* cl = it_cl->second;
        cl->mySend((void*)"plan", 5);
        size_t id_10 = ID;
        cout<<"发送连接计划时候的ID = "<<ID<<endl;
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
    double starttime, endtime;
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
    
    //命令slave节点开始执行子查询
    if(!orderSlaveExecuteSubQuery()){
        cout << "发送执行子查询命令失败" << endl;
        return false;
    }
    
	int flag = 1;
    //发送连接计划
    cout <<"clRef.size() = "<< clRef.size() << endl;
    for(auto a:clRef){
        cout << a.first << "\t" << a.second << endl;
    }

    map<size_t, size_t> executeSubQueryOk;
    for (unordered_map<size_t, client*>::iterator iter = clRef.begin(); iter != clRef.end();iter++) {
        size_t temp = 0;
        iter->second->myRec(&temp);
        cout << "节点" << iter->first << "执行成功?   " << temp << endl;
		executeSubQueryOk[iter->first] = temp;
	}
    if (executeSubQueryOk.size() == clRef.size()) {
        for (auto a : executeSubQueryOk) {
            if (a.second == 0) flag = 0;
        }
    }else flag = 0;
    if (flag == 1) {
        cout << "子节点所有subquery执行完毕，开始发送连接计划" << endl;
        if (!sendPlan()) {
            cout << "发送连接计划失败" << endl;
        }
    }else cout << "子节点执行subquery未完成" << endl;
    
    /*
    {
		map<size_t, string> slaveName;//参数1：slave节点编号，参数2：slave的ip地址
		ifstream in("./host");
		if (!in) {
			cout << "配置文件打开失败" << endl;
			exit(0);
		}
		size_t id;
		in >> id;
		size_t id2;
		string ip;
		while (in >> id2 >> ip) {
			if (id2 > STORE_START_NUM) STORE_COMPUTE_SPLIT = 1;
			if (id2 == id) continue;
			else {
				slaveName[id2] = ip;
			}
		}
		in.close();

		if (STORE_COMPUTE_SPLIT) {
			for (auto sN : slaveName) {
				if (sN.first > STORE_START_NUM) {
					client* cl = new client(sN.second, PORT);
					cl->createSocket();
					cl->myConnect();
					cl->mySend((void*)"closeDB", 8);
					cl->myclose();
					cout << "存储节点slave_" << sN.first << "关闭" << endl;
				}
			}
		}
		else {
			for (auto sN : slaveName) {
				client* cl = new client(sN.second, PORT);
				cl->createSocket();
				cl->myConnect();
				cl->mySend((void*)"closeDB", 8);
				cl->myclose();
				cout << "存储&计算节点slave_" << sN.first << "关闭" << endl;
			}
		}
    }*/
    
    //命令slave节点开始执行连接计划
	map<size_t, size_t> receivePlanOk;
	for (unordered_map<size_t, client*>::iterator iter = clRef.begin(); iter != clRef.end(); iter++) {
		size_t temp = 0;
		iter->second->myRec(&temp);
		cout << "节点" << iter->first << "接收完毕?   " << temp << endl;
        receivePlanOk[iter->first] = temp;
	}
	flag = 1;
    if (receivePlanOk.size() == clRef.size()) {
        for (auto a : receivePlanOk) {
            if (a.second == 0) flag = 0;
        }
    }else flag = 0;
    if (flag == 1) {
        cout << "子节点所有连接计划接收完毕，开始命令执行连接计划" << endl;
        starttime = get_wall_time();
        if(!orderSlaveExecutePlan()){
            cout << "发送命令slave节点执行连接计划出错" << endl;
        }
    }else cout << "子节点接收连接计划未完成" << endl;

    //接收结果
    if(!waitResult()){
	    cout<<"发送结果出错"<<endl;
    }
    endtime = get_wall_time();
    cout << "执行连接计划所用时间:" << endtime - starttime << "秒" << endl;
    return true;
}

//接收结果
bool generalQuery:: waitResult(){

    vector<vector<vector<size_t> > > reVV;
    for(auto cl:clRef){
        //size_t i = 1; i < clRef.size() + 1; i++
        vector<vector<size_t> > reVec;
        //auto it_cl = clRef.find(i);
        //if(it_cl == clRef.end()){
        //    cout<<"客户端不存在："<<i<<endl;
        //    exit(0);
        //}
        //client * cl = it_cl->second;
        size_t idC;
        size_t idS;
        size_t countA;

        cl.second->myRec(&idC);
        if(idC!=0){
            cl.second->myRec(&idS);
            cl.second->myRec(&countA);
            for(size_t j = 0; j < countA; j++){
                size_t line;
                cl.second->myRec(&line);
                vector<size_t> re;
                for(size_t k = 0; k < line; k++){
                    size_t re_1;
                    cl.second->myRec(&re_1);
                    if(re_1 == 0) break;
                    else re.push_back(re_1);
                }
                if(!re.empty()) reVec.push_back(re);
            }
            if(!reVec.empty()) reVV.push_back(reVec);
        }
    }

	for(size_t i = 0; i < reVV.size(); i++){
        for(size_t j = 0; j < reVV[i].size(); j++){
            result.push_back(reVV[i].at(j));
        }
	}
	return true;
}

//返回查询结果
vector<vector<size_t>> generalQuery::getResult()const{
    
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
    map<size_t,size_t> eCount;//(参数1：分区id值，参数2：分区子查询个数)
    for(auto pts:partSub){//计算subquery个数
        //size_t i = 1; i < partSub.size() + 1; i++
        size_t count = 0;
        //auto it_part = partSub.find(i);
        //if(it_part == partSub.end()){
        //    cout<<"分区不存在L："<<i<<endl;
        //    exit(0);
        //}
        //partitionToSub* temp = it_part->second;
        vector<size_t> id = pts.second->getAllSubID();
        count = id.size();
        cout<<"分区"<<pts.first<<"中子查询个数:"<<count<<endl;
        eCount[pts.first]=count;
    }
    
    //此处可以考虑多线程,发送查询语句
    if(STORE_COMPUTE_SPLIT){//存储与计算分离
        for(auto pts:partSub){//根据分区发送
            //size_t i = 1; i < partSub.size() + 1; i++
            if(pts.first > STORE_START_NUM){
                //partitionToSub* temp = pts.second;
                vector<size_t> id = pts.second->getAllSubID();//该分区中所有子查询的id
                //cout<<"分区id个数: "<<id.size()<<endl;
                client* cl = clRef[pts.first];
                size_t count = eCount[pts.first];
                size_t id3 = ID;
                cout<<"总查询ID： "<<ID<<endl;
                cl->mySend((void *)"sentense", 9);  //先发送信号
                cl->mySend(&id3, sizeof(size_t)); //发送查询类ID
                cl->mySend(&count, sizeof(size_t)); //发送子查询个数
                for(size_t j = 0; j < eCount[pts.first]; j++){ //发送子查询
            
                    vector<string> queryStr1 = pts.second->getSubQueryStr(id.at(j));
                    vector<string> queryName = pts.second->getSubQueryName(id.at(j));
            
                    //发送ID
                    size_t id2 = id.at(j);
                    cl->mySend(&id2, sizeof(size_t));
            
                    //发送查询语句
                    cl->mySend((void*) queryStr1.at(0).c_str(), queryStr1.at(0).size());
            
                    //发送变量名数组个数
                    size_t nameNum = queryName.size();
                    cl->mySend(&nameNum, sizeof(size_t));
            
                    //发送变量名
                    for(size_t k = 0; k < queryName.size(); k++){
                        string str1 = queryName.at(k);
                        cout<<"变量名："<<str1<<endl;
                        cl->mySend((void*)str1.c_str(), str1.size());
                    }
                    cout<<"查询语句"<<queryStr1.at(0)<<endl;
                }
            }else{
				client* cl = clRef[pts.first];
				size_t count = eCount[pts.first];//这个为0
				size_t id3 = ID;
				cout << "总查询ID： " << ID << endl;
				cl->mySend((void*)"sentense", 9);
				cl->mySend(&id3, sizeof(size_t));
                cl->mySend(&count, sizeof(size_t));//把这个0发过去，目的是为了仅仅创建一个generalQuery类，不往里边传子查询，因为压根就没有
            }

        }
    }else{//存储与计算不分离
		for (auto pts : partSub) {//根据分区发送
			vector<size_t> id = pts.second->getAllSubID();//该分区中所有子查询的id
			client* cl = clRef[pts.first];
			size_t count = eCount[pts.first];
			size_t id3 = ID;
			cl->mySend((void*)"sentense", 9);  //先发送信号
			cl->mySend(&id3, sizeof(size_t)); //发送查询类ID
			cl->mySend(&count, sizeof(size_t)); //发送子查询个数
			for (size_t j = 0; j < eCount[pts.first]; j++) { //发送子查询

				vector<string> queryStr1 = pts.second->getSubQueryStr(id.at(j));
				vector<string> queryName = pts.second->getSubQueryName(id.at(j));

				//发送ID
				size_t id2 = id.at(j);
				cl->mySend(&id2, sizeof(size_t));

				//发送查询语句
				cl->mySend((void*)queryStr1.at(0).c_str(), queryStr1.at(0).size());

				//发送变量名数组个数
				size_t nameNum = queryName.size();
				cl->mySend(&nameNum, sizeof(size_t));

				//发送变量名
				for (size_t k = 0; k < queryName.size(); k++) {
					string str1 = queryName.at(k);
					cout << "变量名：" << str1 << endl;
					cl->mySend((void*)str1.c_str(), str1.size());
				}
				cout << "查询语句" << queryStr1.at(0) << endl;
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
            cout<<"id :"<<*it_2<<"在"<< it->first<<endl;
        }
    }
    //此时只是将最底层子查询放入了全局映射表中，由最底层查询而union和join生成的父查询没有放进去
    //应该利用连接计划下发时的数据，将父查询也放入全局映射表
    //那么我也可以在连接计划下发的时候对partitionToSub中的subRef进行更改，利用partitionToSub中的addSubref函数
    //这样就可以利用上边的代码把后来生成的子查询也放入到全局映射表中(未完成)

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

    if (_debug_for_szc_) {
        cout << "全局映射表：<queryID,node> =";
        for(auto a:globalIDRef){
            cout << " <" << a.first << "," << a.second << ">";
        }
        cout << endl;
    }

    for(auto cl:clRef){
        cl.second->mySend((void *)"global", 7);  //发送信号
        cl.second->mySend(&id, sizeof(size_t)); //发送总查询ID
        cl.second->mySend(&count, sizeof(size_t)); //发送映射个数
        unordered_map<size_t, size_t>::iterator it;
        for(it = globalIDRef.begin(); it != globalIDRef.end(); it++){
            size_t id1 = it->first;
            size_t node = it->second;
            size_t id_5[2];
            id_5[0] = id1;
            id_5[1] = node;
            cl.second->mySend(&id_5, sizeof(id_5));
        }
    }
    return true;
}

//命令slave节点开始执行子查询
bool generalQuery::orderSlaveExecuteSubQuery(){
    cout << "命令子节点开始执行子查询，总查询id：" <<ID<< endl;
    size_t id = ID;
    //if(STORE_COMPUTE_SPLIT){
    //    for(auto cl:clRef){
    //        if(cl.first > STORE_START_NUM){
    //            cl.second->mySend((void*)"executeSubQuery", 16);
    //            cl.second->mySend(&id, sizeof(size_t));
    //        }
    //    }
    //}else{
        for(auto cl:clRef){
			cl.second->mySend((void*)"executeSubQuery", 16);
			cl.second->mySend(&id, sizeof(size_t));
        }
    //}
    return true;
}

//命令slave节点开始执行连接计划
bool generalQuery::orderSlaveExecutePlan(){
    cout << "命令slave节点开始执行连接计划，总查询id：" << ID << endl;
    size_t id = ID;
    size_t generalRootId = idtosubq.rbegin()->first;//总树根子查询id
    for (auto cl:clRef) {
        cl.second->mySend((void*)"executePlan", 12);
        cl.second->mySend(&id, sizeof(size_t));
        cl.second->mySend(&generalRootId, sizeof(size_t));
    }
    return true;
}