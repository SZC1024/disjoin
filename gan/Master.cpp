/* Master.cpp */
/*
  此文件为Master.h的实现文件
*/
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <climits>
#include <vector>
#include <fstream>

#include <boost/algorithm/string.hpp>

#include "Master.h"

using namespace std;

char *DATABASE_PATH;

// Created by peng on 2019-12-18, 11:48:54
// 映射表和统计信息全局变量
PredicateTable *preTable;
URITable *uriTable;
/// statistics buffer;
StatisticsBuffer **statBuffer[SLAVE_NUM];

/*
  此函数为int转换成string函数专用
*/
string intToString(int temp) {    //int转换成string函数
	char str[100];
	for (int i = 0; i < 100; i++) str[i] = '\0';
	if (temp == 0) {
		str[0] = '0';
		string str1 = str;
		return str1;
	} else {
		int i;
		for (i = 0; temp != 0; i++) {
			int temp_ = temp % 10;
			str[i] = '0' + temp_;
			temp = temp / 10;
		}
		char str_[100];
		for (int j = 0; j < 100; j++) str_[j] = '\0';
		for (int j = i - 1, k = 0; j >= 0; j--, k++)
			str_[k] = str[j];
		
		string str1 = str_;
		cout << "查看string" << str1 << endl;
		return str1;
	}
}

vector <string> readAllQuery(string &fileName) {    //fileName为查询文件绝对路径，例如/home/test，不指定具体文件格式,返回查询文件名
	
	vector <string> result;   //子查询语句string
	string Dir = "./subQuery/";   //Master节点的子查询文件固定保存在subQuery下面
	string file = (Dir + fileName);  //file保存文件的绝对路径
	ifstream in(file.c_str());    //创建读文件对象
	string temp;
	string Str = "";
	if (!in.is_open()) {
		cout << "打开文件失败" << endl;
		return result;
	}
	while (getline(in, temp)) {   //找到第一个子查询的位置
		if (temp == "---") break;
	}
	while (getline(in, temp)) {
		if (temp == "---") {       //关闭旧写入对象，创建新写入对象
			result.push_back(Str);
			cout << "子查询语句" << Str << endl;
			Str = "";
		} else {                       //写入到对应文件对象
			Str = Str + temp;
		}
	}
	return result;
}


/*
  数据分解函数
  CAUTION: 此函数废弃
*/
vector <string> dataDecompose(string &dataFile, string &last) {  //dataFile为数据文件名，last为数据后缀名，例如txt，区分大小写，空格表示无后缀
	string Dir = "./data/"; //原始数据默认存放在./data里面
	string subDir = "./subData/";  //子数据文件存放目录
	string cmdString = "java -cp dis-triplebitCHN-1.0-SNAPSHOT-jar-with-dependencies.jar placer.strategy.HashStrategy ";  //jar包默认放在根目录下
	//string cmdString = "java -cp dis-triplebit-1.0-SNAPSHOT-jar-with-dependencies.jar placer.strategy.HashStrategy ";  //jar包默认放在根目录下
	vector <string> result;
	
	//shell命令
	cmdString += Dir;
	cmdString += dataFile;
	if (last != " ") cmdString += ("." + last);
	cmdString += " ";
	cmdString += subDir;
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "数据分解成功" << endl;
		cout << "命令为：" << cmdString << endl;
		
		for (int i = 0; i < SLAVE_NUM; i++) {    //分解的数据名字，不带格式，3代表数据分解的个数，即slave节点数
			string str = intToString(i);
			string str1 = dataFile + "-" + str;
			result.push_back(str1);
		}
	} else {
		cout << "数据分解失败" << endl;
		cout << "命令为：" << cmdString << endl;
	}
	return result;
}


/*
  查询语句分解函数
*/
string queryDecompose(string &queryFile) {       //queryFile为查询文件名，无后缀，例如lubm2,结果保存在。/subquery的目录下的queryFile文件中
	
	string Dir = "./Query/";
	string subDir = "./subQuery/";
	string result;
	string cmdString = "java -jar ./dis-triplebitCHN-1.0-SNAPSHOT-jar-with-dependencies.jar";
	//string cmdString = "java -jar dis-triplebit-1.0-SNAPSHOT-jar-with-dependencies.jar";
	cmdString += " ";
	cmdString += Dir;
	cmdString += queryFile;
	cmdString += " ";
	cmdString += subDir;
	cmdString += queryFile;
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "查询语句分解成功" << endl;
		cout << "查询分解命令为：" << cmdString << endl;
		result = queryFile;
	} else {
		cout << "查询语句分解失败" << endl;
		cout << "查询分解命令为：" << cmdString << endl;
		result = " ";
	}
	return result;
}

/*
  分解数据发送函数
  从master节点将数据发送到对应slave节点
*/

int sendData(const string &dataFile, string &desSlave, const string &last) {
	//dataFile为文件名，不带格式后缀， desSlave为slave节点名，last为文件格式后缀，例如txt，区分大小写
	//last为空表示无后缀
	string masterDir = "./subData/";   //子数据文件，默认存放在./subData
	string slaveDir = "/opt/disGrace/data/";        //slave节点中数据文件默认存放在data_ZHOU
	
	string cmdString = "scp -r";
	
	cmdString += " ";
	cmdString += masterDir;
	cmdString += dataFile;
	if (last != " ") cmdString += ("." + last);
	cmdString += " ";
	cmdString += "root@";
	cmdString += desSlave;
	cmdString += ":";
	cmdString += slaveDir;
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "数据发送成功" << endl;
		cout << "数据发送命令:" << cmdString << endl;
		return 1;
	} else {
		cout << "数据发送失败" << endl;
		cout << "数据发送命令:" << cmdString << endl;
	}
	return 0;
}

/*
  查询语句发送函数
  将Master节点的子查询文件发送到对应slave节点
  CAUTION: 此函数废弃
*/

int sendQuery(string &queryFile, string &desSlave) {       //queryFile为查询文件名，desslave为目的主机名
	
	string masterDir = "./subQuery/";    //master中子查询固定存放目录
	string slaveDir = "/opt/Build/subQuery_zhou/";    //slave节点中子查询语句固定存放目录
	
	string cmdString = "scp -r";   //shell命令
	
	cmdString += " ";
	cmdString += masterDir;
	cmdString += queryFile;
	cmdString += " ";
	cmdString += "root@";
	cmdString += desSlave;
	cmdString += ":";
	cmdString += slaveDir;
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "查询文件发送成功" << endl;
		cout << "查询发送命令:" << cmdString << endl;
		return 1;
	} else {
		cout << "查询发送失败" << endl;
		cout << "查询发送命令:" << cmdString << endl;
	}
	return 0;
}

/*
  在单个slave节点上执行单个查询
*/

string executeSingleQueryToOneSlave(string &queryFile, string &slaveName) {    //queryFile为查询文件名，返回所结果所存放的文件名，不带后缀
	
	string slaveQuery = "/home/zhouhuajian/Build/subQuery_zhou/";    //slave节点中子查询保存目录
	string slaveResult = "/home/zhouhuajian/Build/oldDatabaseQueryResult/";   //slave节点中子查询结果保存目录
	string slaveAPI = "/home/zhouhuajian/Build/bin/lrelease/searchOldDatabase";             //slave节点中可执行文件路径
	string cmdString = "ssh";
	string result;
	
	cmdString += " ";
	cmdString += slaveName;
	cmdString += " ";
	cmdString += "\"";
	cmdString += slaveAPI;
	cmdString += " ";
	cmdString += slaveQuery;
	cmdString += queryFile;
	cmdString += " ";
	cmdString += slaveResult;
	cmdString += queryFile;
	cmdString += "\"";
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "执行查询文件成功" << endl;
		cout << "执行查询命令:" << cmdString << endl;
		result = queryFile;
	} else {
		cout << "执行查询文件失败" << endl;
		cout << "执行查询命令:" << cmdString << endl;
		result = " ";
	}
	return result;
}

/*
  将查询结果发送到Master节点对应目录
*/

int sendResultToMaster(string &queryResult, string &slaveName) {  //queryFile为查询结果名，无后缀，slaveName为slave节点名
	
	string masterDir = "/home/zhouhuajian/test/queryResult/";   //master节点中结果文件保存目录
	string slaveDir = "/home/zhouhuajian/Build/oldDatabaseQueryResult/";   //slave节点中结果存放目录
	
	string cmdString = "ssh";
	
	cmdString += " ";
	cmdString += slaveName;
	cmdString += " ";
	cmdString += "\"";
	cmdString += "scp -r";
	cmdString += " ";
	cmdString += slaveDir;
	cmdString += queryResult;
	cmdString += " ";
	cmdString += "root@";
	cmdString += "Master";
	cmdString += ":";
	cmdString += masterDir;
	cmdString += slaveName;
	//cmdString += "/";
	cmdString += "\"";
	
	int pid;
	pid = system(cmdString.c_str());
	
	if (pid == 0) {
		cout << "发送结果文件到master文件成功" << endl;
		cout << "查询发送命令:" << cmdString << endl;
		return 1;
	} else {
		cout << "发送结果文件到master节点失败" << endl;
		cout << "查询发送命令:" << cmdString << endl;
	}
	
	return 0;
}

/*
  在所有节点上执行所有查询，并将结果以vector的形式返回
*/
vector <string> executeQuery(vector <string> &queryFile, vector <string> &slaveName) {
	vector <string> queryResultFile;
	vector <string> result;
	for (vector<string>::size_type i = 0; i < slaveName.size(); i++) {   //执行所有子查询
		for (vector<string>::size_type j = 0; j < queryFile.size(); j++) {
			if (i == 0) {
				string str = executeSingleQueryToOneSlave(queryFile.at(j), slaveName.at(i));
				queryResultFile.push_back(str);
			} else executeSingleQueryToOneSlave(queryFile.at(j), slaveName.at(i));
		}
	}
	
	for (vector<string>::size_type i = 0; i < slaveName.size(); i++) {    //将结果发送到master节点
		for (vector<string>::size_type j = 0; j < queryResultFile.size(); j++) {
			sendResultToMaster(queryResultFile.at(j), slaveName.at(i));
		}
	}
	
	//执行spark部分
	result = executeSpark(queryFile, slaveName);
	
	return result;
}

/*
 执行spark部分，并获取结果
 resultFile为查询结果文件名，保存在Maseter节点的./queryResult中的对应slave目录下
*/
vector <string> executeSpark(vector <string> &resultFile, vector <string> &slaveName) {
	vector <string> result;    //结果
	string Dir = "./queryResult/";
	
	//结果文件读取生成Vector
	vector <vector<string>> resultVec;
	
	
	return result;
}

/*
  提供给上层web的接口
*/
vector <string> allOperationOfQuery(string &QueryFile) {  //QueryFile为文件名，不带后缀
	
	vector <string> result;   //结果
	vector <string> slaveName;  //slave节点名字
	string allSubQuery;      //子查询汇总文件
	vector <string> subQuery;  //子查询文件
	
	//赋值slave节点名
	slaveName.push_back("slave1");
	slaveName.push_back("slave2");
	slaveName.push_back("slave3");
	
	//查询分解
	allSubQuery = queryDecompose(QueryFile);
	
	//将子查询总文件分解成多个文件
	subQuery = readAllQuery(allSubQuery);
	
	//将子查询文件分发到各个节点
	for (vector<string>::size_type i = 0; i < slaveName.size(); i++) {
		for (vector<string>::size_type j = 0; j < subQuery.size(); j++)
			sendQuery(subQuery.at(j), slaveName.at(i));
	}
	
	//执行查询文件
	result = executeQuery(subQuery, slaveName);
	
	return result;
}


/*
 在某个节点将数据文件加载成数据库
 dataFile是数据文件名，固定保存在。/subData目录下
 slaveName是节点名
 last是数据文件后缀格式，若为空代表无后缀
*/
int createSingleDatabase(const string &dataFile, string &slaveName, const string &last) {
	
	//string slaveAPI = "/opt/Grace/bin/lrelease/buildTripleBitFromCHN";   //创建数据库API所在
	string slaveAPI = "/opt/disGrace/bin/lrelease/buildTripleBitFromN3";   //创建数据库API所在
	string dataDir = "/opt/disGrace/data/"; //数据文件所在目录
	string databaseDir = "/opt/disGrace/mydatabase";  //数据库文件目录
	string cmdString = "ssh";                      //shell命令
	cmdString += " ";
	cmdString += slaveName;
	cmdString += " ";
	cmdString += "\"";
	cmdString += slaveAPI;
	cmdString += " ";
	cmdString += dataDir;
	cmdString += dataFile;
	if (last != " ") cmdString += ("." + last);  //若last不为空，加上后缀
	cmdString += " ";
	cmdString += databaseDir;
	cmdString += "\"";
	
	int pid;
	const char *cmdChar = cmdString.c_str();
	pid = system(cmdChar);
	
	if (pid == 0) {
		cout << slaveName << " 创建数据库成功" << endl;
		cout << cmdString << endl;
	} else {
		cout << slaveName << " 数据库创建失败" << endl;
		cout << cmdString << endl;
	}
	return 0;
}

/*
 将初始数据文件进行分解，并生成分布式数据库
 dataFile为初始数据文件名，不带后缀，默认保存在./data目录下
 slaveName为节点名
 last为文件后缀格式名，如txt
*/
int createDatabase(string &dataFile, vector <string> &slaveName, string &last, string targetDir) {
	
	DATABASE_PATH = (char *) malloc(sizeof(char) * targetDir.size());
	strcpy(DATABASE_PATH, targetDir.c_str());
	// Created by peng on 2019-12-15, 15:36:10
	// TODO: 数据分解前需要做ID全局映射
	TripleBitBuilder *builder = new TripleBitBuilder(targetDir);
	
	long long pos;
	for (pos = (long long) dataFile.size(); pos >= 0; --pos) {
		if (dataFile[pos] == '/') {
			break;
		}
	}
	string fileName = dataFile.substr(pos + 1);
	
	TempFile rawFacts(targetDir + "/" + fileName + "-rawFacts");
	builder->startBuild(dataFile + "." + last, targetDir, rawFacts);
	//formatTempFile(rawFacts, targetDir);
	rawFacts.close();
	
	TempFile **slavesTempFile = (TempFile **) malloc(sizeof(TempFile *) * SLAVE_NUM);
	for (int i = 0; i < SLAVE_NUM; ++i) {
		slavesTempFile[i] = new TempFile(targetDir + "/" + fileName + "-slave-" + std::to_string(i));
	}
	cout << "rawFacts's file name: " + rawFacts.getFile() << endl;
	for (int i = 0; i < SLAVE_NUM; ++i) {
		cout << "slavesTempFile-" << i << "'s file name: " << slavesTempFile[i]->getFile() << endl;
	}
	realDataDecompose(rawFacts, slavesTempFile);
	
	// Created by peng on 2019-12-15, 21:38:54
	// FIXME：数据分解之后应该生成多个rawFacts文件，每个slave节点一个，
	//  在slave节点直接调用完整的resolveTriples函数(bitmap插入未被删除的函数)
	//  resolveTriples函数需要去除tempfile中的重复三元组
	for (int i = 0; i < SLAVE_NUM; ++i) {
		builder->resolveTriples(*slavesTempFile[i], i);
	}
	builder->endBuild();
	delete builder;
	
	/*for (int i = 0; i < SLAVE_NUM; ++i) {
		formatTempFile(*slavesTempFile[i], targetDir);
	}*/
	
	for (int i = 0; i < SLAVE_NUM; ++i) {
		delete slavesTempFile[i];
	}
	
	// Created by peng on 2019-12-23, 15:38:07
	// 加载table和索引，并测试查询语句转换，释放table和索引
	/*loadTableAndStatistic(targetDir);
	
	for (unsigned i = 1; i <= 8; ++i) {
		ifstream ifs("queryLUBM" + std::to_string(i));
		string line;
		string queryStr = "";
		while (getline(ifs, line)) {
			queryStr += line + '\n';
		}
		cout << transformQuery(queryStr) << endl;
	}
	
	releaseTableAndStatistic();*/
	
	//分解数据文件
	//subDataFile = dataDecompose(dataFile, last);
	
	//发送数据文件
	/*for (vector<string>::size_type i = 0; i < SLAVE_NUM; i++) {
		sendData(slavesTempFile[i]->getFile(), slaveName.at(i), " ");
	}*/
	
	//创建数据库，可用多线程
	/*for (vector<string>::size_type i = 0; i < subDataFile.size(); i++) {
		createSingleDatabase(slavesTempFile[i]->getFile(), slaveName.at(i), " ");
	}*/
	
	return 1;
}

int loadTableAndStatistic(const string &dir) {
	preTable = PredicateTable::load(dir);
	uriTable = URITable::load(dir);
	MMapBuffer *indexBufferFile[SLAVE_NUM];
	for (int i = 0; i < SLAVE_NUM; ++i) {
		string fileName = dir + "/statIndex-" + std::to_string(i);
		cerr << "MMapBuffer " + fileName + " create" << endl;
		indexBufferFile[i] = MMapBuffer::create(fileName.c_str(), 0);
		statBuffer[i] = (StatisticsBuffer **) malloc(sizeof(StatisticsBuffer *) * 4);
	}
	for (int i = 0; i < SLAVE_NUM; ++i) {
		char *indexBuffer = indexBufferFile[i]->get_address();
		
		string statFilename = dir + "/subject_statis-" + std::to_string(i);
		statBuffer[i][0] = OneConstantStatisticsBuffer::load(StatisticsBuffer::SUBJECT_STATIS, statFilename,
		                                                     indexBuffer);
		
		statFilename = dir + "/object_statis-" + std::to_string(i);
		statBuffer[i][1] = OneConstantStatisticsBuffer::load(StatisticsBuffer::OBJECT_STATIS, statFilename,
		                                                     indexBuffer);
		
		statFilename = dir + "/subjectpredicate_statis-" + std::to_string(i);
		statBuffer[i][2] = TwoConstantStatisticsBuffer::load(StatisticsBuffer::SUBJECTPREDICATE_STATIS, statFilename,
		                                                     indexBuffer);
		
		statFilename = dir + "/objectpredicate_statis-" + std::to_string(i);
		statBuffer[i][3] = TwoConstantStatisticsBuffer::load(StatisticsBuffer::OBJECTPREDICATE_STATIS, statFilename,
		                                                     indexBuffer);
	}
	for (int i = 0; i < SLAVE_NUM; ++i) {
		delete indexBufferFile[i];
	}
	return 0;
}

int releaseTableAndStatistic() {
	if (uriTable != NULL) {
		delete uriTable;
	}
	uriTable = NULL;
	
	if (preTable != NULL) {
		delete preTable;
	}
	preTable = NULL;
	
	for (int i = 0; i < SLAVE_NUM; ++i) {
		for (int j = 0; j < 4; ++j) {
			if (statBuffer[i][j] != NULL) {
				delete statBuffer[i][j];
			}
			statBuffer[i][j] = NULL;
		}
		delete statBuffer[i];
	}
	return 0;
}

/**
 * 由于rawFacts文件格式问题，使用原来的java实现需要对文件进行多次读写
 * 考虑到可能存在的性能问题，把原来数据分解功能用该函数进行简单实现
 * 注意：本函数未处理同一slave节点重复放置三元组的问题。
 * 重复三元组问题由TripleBitBuilder::resolveTriples负责解决。
 * @param rawFacts ID数据文件
 */
void realDataDecompose(TempFile &rawFacts, TempFile **slavesTempFile) {
	MemoryMappedFile mappedIn;
	assert(mappedIn.open(rawFacts.getFile().c_str()));
	ID s, p, o;
	
	const char *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
	
	// Created by peng on 2019-12-16, 15:19:37
	// 先从rawFacts文件中读取三元组
	while (reader < limit) {
		TripleBitBuilder::loadTriple(reader, s, p, o);
		int i = s % SLAVE_NUM;
		int j = o % SLAVE_NUM;
		slavesTempFile[i]->writeId(s);
		slavesTempFile[i]->writeId(p);
		slavesTempFile[i]->writeId(o);
		if (i != j) {
			slavesTempFile[j]->writeId(s);
			slavesTempFile[j]->writeId(p);
			slavesTempFile[j]->writeId(o);
		}
		reader += sizeof(ID) * 3;
	}
	return;
}

/**
 * 将uri的查询语句转换成ID类型的查询语句
 * @param query 常量为uri的查询语句
 * @return 转换后的查询语句
 */
string transformQuery(string query) {
	SPARQLLexer *lexer = new SPARQLLexer(query);
	SPARQLParser *parser = new SPARQLParser(*lexer);
	try {
		parser->parse();
	} catch (const SPARQLParser::ParserException &e) {
		cout << "Parser error: " << e.message << endl;
		return NULL;
	}
	
	SPARQLParser::PatternGroup &patternGroup = parser->getPatterns();
	for (unsigned long i = 0; i < patternGroup.patterns.size(); ++i) {
		SPARQLParser::Pattern &pattern = patternGroup.patterns[i];
		if (pattern.subject.type != SPARQLParser::Element::Variable) {
			ID id;
			uriTable->getIdByURI(pattern.subject.value.c_str(), id);
			pattern.subject.value = std::to_string(id);
		}
		if (pattern.predicate.type != SPARQLParser::Element::Variable) {
			ID id;
			preTable->getIDByPredicate(pattern.predicate.value.c_str(), id);
			pattern.predicate.value = std::to_string(id);
		}
		if (pattern.object.type != SPARQLParser::Element::Variable) {
			ID id;
			uriTable->getIdByURI(pattern.object.value.c_str(), id);
			pattern.object.value = std::to_string(id);
		}
	}
	
	return generateTransformedQuery(parser);
}

string generateTransformedQuery(SPARQLParser *parser) {
	string rst = "select";
	std::map<std::string, unsigned> &nameMap = parser->getNamedVariables();
	std::map<unsigned, std::string> reverseMap;
	
	std::map<std::string, unsigned>::iterator begin;
	for (begin = nameMap.begin(); begin != nameMap.end(); ++begin) {
		reverseMap[begin->second] = begin->first;
	}
	
	std::vector<unsigned> &projection = parser->getProjection();
	for (unsigned i = 0; i < projection.size(); ++i) {
		unsigned varID = projection[i];
		rst += " ?" + reverseMap[varID];
	}
	rst += " where {\n";
	
	SPARQLParser::PatternGroup &patternGroup = parser->getPatterns();
	for (unsigned long i = 0; i < patternGroup.patterns.size(); ++i) {
		SPARQLParser::Pattern &pattern = patternGroup.patterns[i];
		/*if (pattern.subject.type != SPARQLParser::Element::Variable) {
			rst += "    <" + pattern.subject.value + ">";
		} else {
			rst += "    ?" + reverseMap[pattern.subject.id];
		}
		if (pattern.predicate.type != SPARQLParser::Element::Variable) {
			rst += " <" + pattern.predicate.value + ">";
		} else {
			rst += " ?" + reverseMap[pattern.predicate.id];
		}
		if (pattern.object.type != SPARQLParser::Element::Variable) {
			rst += " <" + pattern.object.value + ">";
		} else {
			rst += " ?" + reverseMap[pattern.object.id];
		}
		rst += " .\n";*/
		if (pattern.subject.type != SPARQLParser::Element::Variable) {
			rst += "    " + pattern.subject.value + "";
		} else {
			rst += "    ?" + reverseMap[pattern.subject.id];
		}
		if (pattern.predicate.type != SPARQLParser::Element::Variable) {
			rst += " " + pattern.predicate.value + "";
		} else {
			rst += " ?" + reverseMap[pattern.predicate.id];
		}
		if (pattern.object.type != SPARQLParser::Element::Variable) {
			rst += " " + pattern.object.value + "";
		} else {
			rst += " ?" + reverseMap[pattern.object.id];
		}
		rst += " .\n";
	}
	rst += '}';
	return rst;
}

ID getIDByUri(const string uri) {
	ID id;
	Status status = uriTable->getIdByURI(uri.c_str(), id);
	if (status == URI_NOT_FOUND) {
		cerr << "master/Master.cpp:588: URI_NOT_FOUND" << endl;
		id = -1;
	}
	return id;
}

string getUriByID(ID id) {
	string uri;
	Status status = uriTable->getURIById(uri, id);
	if (status == URI_NOT_FOUND) {
		uri = "";
		cerr << "master/Master.cpp:599: URI_NOT_FOUND" << endl;
	}
	return uri;
}

ID getIDByPredicate(const string pre) {
	ID id;
	Status status = preTable->getIDByPredicate(pre.c_str(), id);
	if (status == PREDICATE_NOT_BE_FINDED) {
		cerr << "master/Master.cpp:608: PREDICATE_NOT_BE_FINDED" << endl;
		id = -1;
	}
	return id;
}

string getPredicateByID(ID pid) {
	string pre = preTable->getPrediacateByID(pid);
	if (pre.size() == 0) {
		cerr << "master/Master.cpp:617: PREDICATE_NOT_BE_FINDED" << endl;
	}
	return pre;
}

size_t getResultSize(string query, size_t slaveID) {
	SPARQLLexer *lexer = new SPARQLLexer(query);
	SPARQLParser *parser = new SPARQLParser(*lexer);
	try {
		parser->parse();
	} catch (const SPARQLParser::ParserException &e) {
		cout << "Parser error: " << e.message << endl;
		return UINT_MAX;
	}
	
	std::map<std::string, unsigned> &nameMap = parser->getNamedVariables();
	std::map<unsigned, std::string> reverseMap;
	
	std::map<std::string, unsigned>::iterator begin;
	for (begin = nameMap.begin(); begin != nameMap.end(); ++begin) {
		reverseMap[begin->second] = begin->first;
	}
	
	size_t rst = UINT_MAX;
	bool flag = false;
	SPARQLParser::PatternGroup &patternGroup = parser->getPatterns();
	
	for (unsigned long i = 0; i < patternGroup.patterns.size(); ++i) {
		SPARQLParser::Pattern &pattern = patternGroup.patterns[i];
		
		if (pattern.subject.type == SPARQLParser::Element::Variable) {
			// Created by peng on 2019-12-21, 19:35:04
			// 主语是变量
			if (pattern.object.type == SPARQLParser::Element::Variable) {
				// Created by peng on 2019-12-21, 19:35:28
				// 宾语也是变量
				continue;
			} else {
				if (pattern.predicate.type == SPARQLParser::Element::Variable) {
					// Created by peng on 2019-12-21, 19:33:43
					// 主变，宾非变，谓变
					ID id, waste = 0;
					uriTable->getIdByURI(pattern.object.value.c_str(), id);
					statBuffer[slaveID][1]->getStatis(id, waste);
					if (id < rst) {
						rst = id;
						flag = true;
					}
				} else {
					// Created by peng on 2019-12-21, 19:34:14
					// 主变，宾非变，谓非变
					ID id, pid;
					uriTable->getIdByURI(pattern.object.value.c_str(), id);
					preTable->getIDByPredicate(pattern.predicate.value.c_str(), pid);
					statBuffer[slaveID][3]->getStatis(id, pid);
					if (id < rst) {
						rst = id;
						flag = true;
					}
				}
			}
		} else {
			// Created by peng on 2019-12-21, 19:35:17
			// 主语不是变量
			if (pattern.object.type == SPARQLParser::Element::Variable) {
				// Created by peng on 2019-12-21, 19:35:28
				// 宾语是变量
				if (pattern.predicate.type == SPARQLParser::Element::Variable) {
					// Created by peng on 2019-12-21, 19:49:29
					// 主非变，宾变，谓变
					ID id, waste;
					uriTable->getIdByURI(pattern.subject.value.c_str(), id);
					statBuffer[slaveID][0]->getStatis(id, waste);
					if (id < rst) {
						rst = id;
						flag = true;
					}
				} else {
					// Created by peng on 2019-12-21, 19:50:15
					// 主非变，宾变，谓非变
					ID id, pid;
					uriTable->getIdByURI(pattern.subject.value.c_str(), id);
					preTable->getIDByPredicate(pattern.predicate.value.c_str(), pid);
					statBuffer[slaveID][2]->getStatis(id, pid);
					if (id < rst) {
						rst = id;
						flag = true;
					}
				}
			} else {
				if (pattern.predicate.type == SPARQLParser::Element::Variable) {
					// Created by peng on 2019-12-21, 19:33:43
					// 主非变，宾非变，谓变
					// 可能有一条或多条
					return 1;
				} else {
					// Created by peng on 2019-12-21, 19:34:14
					// 主非变，宾非变，谓非变
					// 全是常量可能在子节点有也可能没有
					return 1;
				}
			}
		}
	}
	if (flag) {
		return rst;
	}
	return 0;
}

/**
 * 将rawFacts格式的ID文件变成文本形式的用空格分隔的ID三元组文件
 * FIXME:此函数废弃未使用
 * @param rawFacts 原始二进制ID文件
 * @param targetDir 目标文件存放目录
 */
void formatTempFile(TempFile &rawFacts, const string &targetDir) {
	MemoryMappedFile mappedIn;
	cout << "formatTempFile -" << rawFacts.getFile() << endl;
	assert(mappedIn.open(rawFacts.getFile().c_str()));
	ID s, p, o;
	
	const char *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
	
	ofstream outFile(rawFacts.getFile() + ".ff");
	while (reader < limit) {
		s = *(ID *) reader;
		reader += sizeof(ID);
		p = *(ID *) reader;
		reader += sizeof(ID);
		o = *(ID *) reader;
		reader += sizeof(ID);
		/*outFile << "\"" << s << "\"" << " "
		        << "\"" << p << "\"" << " "
		        << "\"" << o << "\"" << endl;*/
		outFile << "<" << s << ">" << " "
		        << "<" << p << ">" << " "
		        << "\"" << o << "\"" << " ." << endl;
	}
	outFile.close();
}

/*
  开启slave节点上的服务器监听
*/

void startServer(vector <string> slaveName) {
	
	string cmdString = "ssh";
	
	string Dir = "/opt/Build/bin/lrelease/searchOldDatabase";
	
	for (vector<string>::size_type i = 0; i < slaveName.size(); i++) {
		
		int pid;
		string str = cmdString + " ";
		str += slaveName.at(i);
		str += " ";
		str += "\"";
		str += Dir;
		str += "\"";
		
		pid = system(str.c_str());
		if (pid == 0) {
			cout << slaveName.at(i) << "开启监听成功" << endl;
		} else cout << slaveName.at(i) << "开启监听失败" << endl;
	}
}

/* Master.cpp */
