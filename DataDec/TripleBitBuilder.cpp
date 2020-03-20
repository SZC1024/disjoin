/*
 * TripleBitBuilder.cpp
 *
 *  Created on: Apr 6, 2010
 *      Author: root
 */

#include "TripleBitBuilder.h"
#include "MemoryBuffer.h"
#include "MMapBuffer.h"
#include "PredicateTable.h"
#include "URITable.h"
#include "Sorter.h"

#include <string.h>
#include <pthread.h>

static int getCharPos(const char *data, char ch) {
	const char *p = data;
	int i = 0;
	while (*p != '\0') {
		if (*p == ch)
			return i + 1;
		p++;
		i++;
	}
	
	return -1;
}

TripleBitBuilder::TripleBitBuilder(string _dir) : dir(_dir) {
	std::cout << "TripleBitBuilder::TripleBitBuilder" << std::endl;
	preTable = new PredicateTable(dir);
	uriTable = new URITable(dir);
	//bitmap = new BitmapBuffer(dir);
	
	for (int i = 0; i < SLAVE_NUM; ++i) {
		statBuffer[i] = (StatisticsBuffer **) malloc(sizeof(StatisticsBuffer *) * 4);
		statBuffer[i][0] = new OneConstantStatisticsBuffer(string(dir + "/subject_statis-" + std::to_string(i)),
		                                                   StatisticsBuffer::SUBJECT_STATIS);
		statBuffer[i][1] = new OneConstantStatisticsBuffer(string(dir + "/object_statis-" + std::to_string(i)),
		                                                   StatisticsBuffer::OBJECT_STATIS);
		statBuffer[i][2] = new TwoConstantStatisticsBuffer(
				string(dir + "/subjectpredicate_statis-" + std::to_string(i)),
				StatisticsBuffer::SUBJECTPREDICATE_STATIS);
		statBuffer[i][3] = new TwoConstantStatisticsBuffer(string(dir + "/objectpredicate_statis-" + std::to_string(i)),
		                                                   StatisticsBuffer::OBJECTPREDICATE_STATIS);
	}
	
	staReifTable = new StatementReificationTable();
}

TripleBitBuilder::TripleBitBuilder() {
	preTable = NULL;
	uriTable = NULL;
	//bitmap = NULL;
	staReifTable = NULL;
}

TripleBitBuilder::~TripleBitBuilder() {
#ifdef TRIPLEBITBUILDER_DEBUG
	cout << "Bit map builder destroyed begin " << endl;
#endif
	//mysql = NULL;
	if (preTable != NULL)
		delete preTable;
	preTable = NULL;
	
	if (uriTable != NULL)
		delete uriTable;
	uriTable = NULL;
	//delete uriStaBuffer;
	if (staReifTable != NULL)
		delete staReifTable;
	staReifTable = NULL;
	
	/*if (bitmap != NULL) {
		delete bitmap;
		bitmap = NULL;
	}
	
	for (int i = 0; i < 4; i++) {
		if (statBuffer[i] != NULL)
			delete statBuffer[i];
		statBuffer[i] = NULL;
	}*/
	for (int i = 0; i < SLAVE_NUM; ++i) {
		for (int j = 0; j < 4; ++j) {
			if (statBuffer[i][j] != NULL) {
				delete statBuffer[i][j];
			}
			statBuffer[i][j] = NULL;
		}
	}
}

bool TripleBitBuilder::isStatementReification(const char *object) {
	int pos;
	
	const char *p;
	
	if ((pos = getCharPos(object, '#')) != -1) {
		p = object + pos;
		
		if (strcmp(p, "Statement") == 0 || strcmp(p, "subject") == 0 || strcmp(p, "predicate") == 0 ||
		    strcmp(p, "object") == 0) {
			return true;
		}
	}
	
	return false;
}

bool TripleBitBuilder::generateXY(ID &subjectID, ID &objectID) {
	if (subjectID > objectID) {
		ID temp = subjectID;
		subjectID = objectID;
		objectID = temp - objectID;
		return true;
	} else {
		objectID = objectID - subjectID;
		return false;
	}
}

void TripleBitBuilder::NTriplesParse(const char *subject, const char *predicate, const char *object, TempFile &rawFacts) {
	ID subjectID, objectID, predicateID;
	
	if (isStatementReification(object) == false && isStatementReification(predicate) == false) {
		if (preTable->getIDByPredicate(predicate, predicateID) == PREDICATE_NOT_BE_FINDED) {
			preTable->insertTable(predicate, predicateID);
		}
		if (uriTable->getIdByURI(subject, subjectID) == URI_NOT_FOUND)
			uriTable->insertTable(subject, subjectID);
		if (uriTable->getIdByURI(object, objectID) == URI_NOT_FOUND)
			uriTable->insertTable(object, objectID);
		
		rawFacts.writeId(subjectID);
		rawFacts.writeId(predicateID);
		rawFacts.writeId(objectID);
	} else {
//		statementFile << subject << " : " << predicate << " : " << object << endl;
	}
	
}

bool TripleBitBuilder::N3Parse(istream &in, const char *name, TempFile &rawFacts) {
	cerr << "Parsing " << name << "..." << endl;
	
	TurtleParser parser(in);
	try {
		string subject, predicate, object;
		while (true) {
			try {
				if (!parser.parse(subject, predicate, object))
					break;
			} catch (const TurtleParser::Exception &e) {
				while (in.get() != '\n');
				continue;
			}
			//Construct IDs
			//and write the triples
			if (subject.length() && predicate.length() && object.length())
				NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), rawFacts);
			
		}
	} catch (const TurtleParser::Exception &) {
		return false;
	}
	cerr << "Parsing End: " << name << " !!!" << endl;
	return true;
}

const char *TripleBitBuilder::skipIdIdId(const char *reader) {
	return TempFile::skipId(TempFile::skipId(TempFile::skipId(reader)));
}

int TripleBitBuilder::compare213(const char *left, const char *right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	
	return cmpTriples(l2, l1, l3, r2, r1, r3);
}

int TripleBitBuilder::compare231(const char *left, const char *right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	
	return cmpTriples(l2, l3, l1, r2, r3, r1);
}

int TripleBitBuilder::compare123(const char *left, const char *right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	
	return cmpTriples(l1, l2, l3, r1, r2, r3);
}

int TripleBitBuilder::compare321(const char *left, const char *right) {
	ID l1, l2, l3, r1, r2, r3;
	loadTriple(left, l1, l2, l3);
	loadTriple(right, r1, r2, r3);
	
	return cmpTriples(l3, l2, l1, r3, r2, r1);
}

/**
 * 该函数目前删除了bitmap插入部分，因此该函数现仅用于插入数据的统计信息
 * TODO: 统计信息部分待更新
 * @param rawFacts 存放着ID三元组的文件
 * @param facts 该参数未使用到，历史遗留
 * @return
 */
Status TripleBitBuilder::resolveTriples(TempFile &rawFacts, int slaveID) {
	cout << "Sort by Subject" << slaveID << endl;
	ID subjectID, objectID, predicateID;
	
	ID lastSubject = 0, lastObject = 0, lastPredicate = 0;
	unsigned count0 = 0, count1 = 0;
	TempFile sortedBySubject("./SortByS"), sortedByObject("./SortByO");
	Sorter::sort(rawFacts, sortedBySubject, skipIdIdId, compare123);
	{
		//insert into chunk
		sortedBySubject.close();
		MemoryMappedFile mappedIn;
		assert(mappedIn.open(sortedBySubject.getFile().c_str()));
		const char *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
		
		loadTriple(reader, subjectID, predicateID, objectID);
		lastSubject = subjectID;
		lastPredicate = predicateID;
		lastObject = objectID;
		reader = skipIdIdId(reader);
		generateXY(subjectID, objectID);
		// Created by peng on 2019-12-15, 17:05:29
		// 注释掉所有insertTriple函数，因为插入数据到chunk这部分在slave节点完成
		// bitmap->insertTriple(predicateID, subjectID, objectID, v, 0);
		count0 = count1 = 1;
		
		while (reader < limit) {
			loadTriple(reader, subjectID, predicateID, objectID);
			if (lastSubject == subjectID && lastPredicate == predicateID && lastObject == objectID) {
				reader = skipIdIdId(reader);
				continue;
			}
			
			if (subjectID != lastSubject) {
				((OneConstantStatisticsBuffer *) statBuffer[slaveID][0])->addStatis(lastSubject, count0);
				statBuffer[slaveID][2]->addStatis(lastSubject, lastPredicate, count1);
				lastPredicate = predicateID;
				lastSubject = subjectID;
				count0 = count1 = 1;
			} else if (predicateID != lastPredicate) {
				statBuffer[slaveID][2]->addStatis(lastSubject, lastPredicate, count1);
				lastPredicate = predicateID;
				count0++;
				count1 = 1;
			} else {
				count0++;
				count1++;
				lastObject = objectID;
			}
			
			reader = reader + 12;
			generateXY(subjectID, objectID);
			// 0 indicate the triple is sorted by subjects' id;
			// bitmap->insertTriple(predicateID, subjectID, objectID, v, 0);
		}
		mappedIn.close();
	}
	
	//bitmap->flush();
	
	//sort
	cerr << "Sort by Object" << endl;
	Sorter::sort(rawFacts, sortedByObject, skipIdIdId, compare321);
	{
		//insert into chunk
		sortedByObject.close();
		MemoryMappedFile mappedIn;
		assert(mappedIn.open(sortedByObject.getFile().c_str()));
		const char *reader = mappedIn.getBegin(), *limit = mappedIn.getEnd();
		
		loadTriple(reader, subjectID, predicateID, objectID);
		lastSubject = subjectID;
		lastPredicate = predicateID;
		lastObject = objectID;
		reader = skipIdIdId(reader);
		generateXY(objectID, subjectID);
		//bitmap->insertTriple(predicateID, objectID, subjectID, v, 1);
		count0 = count1 = 1;
		
		while (reader < limit) {
			loadTriple(reader, subjectID, predicateID, objectID);
			if (lastSubject == subjectID && lastPredicate == predicateID && lastObject == objectID) {
				reader = skipIdIdId(reader);
				continue;
			}
			
			if (objectID != lastObject) {
				((OneConstantStatisticsBuffer *) statBuffer[slaveID][1])->addStatis(lastObject, count0);
				statBuffer[slaveID][3]->addStatis(lastObject, lastPredicate, count1);
				lastPredicate = predicateID;
				lastObject = objectID;
				count0 = count1 = 1;
			} else if (predicateID != lastPredicate) {
				statBuffer[slaveID][3]->addStatis(lastObject, lastPredicate, count1);
				lastPredicate = predicateID;
				count0++;
				count1 = 1;
			} else {
				lastSubject = subjectID;
				count0++;
				count1++;
			}
			reader = skipIdIdId(reader);
			generateXY(objectID, subjectID);
			// 1 indicate the triple is sorted by objects' id;
			//bitmap->insertTriple(predicateID, objectID, subjectID, v, 1);
		}
		mappedIn.close();
	}
	
	//bitmap->flush();
	
	// Created by peng on 2019-12-15, 21:05:07
	// 按照写代码的原则，这个rawFacts是从外面传进来的，所以此处discard应该写在外部
	// rawFacts.discard();
	sortedByObject.discard();
	sortedBySubject.discard();
	
	return OK;
}

Status TripleBitBuilder::startBuildN3(string fileName) {
	std::cout << "TripleBitBuilder::startBuildN3" << std::endl;
	TempFile rawFacts("./test");
	
	ifstream in((char *) fileName.c_str());
	if (!in.is_open()) {
		cerr << "Unable to open " << fileName << endl;
		return ERROR;
	}
	if (!N3Parse(in, fileName.c_str(), rawFacts)) {
		in.close();
		return ERROR;
	}
	
	in.close();
	delete uriTable;
	uriTable = NULL;
	delete preTable;
	preTable = NULL;
	delete staReifTable;
	staReifTable = NULL;
	
	rawFacts.flush();
	cout << "over" << endl;
	
	//sort by s,o
	TempFile facts(fileName);
	resolveTriples(rawFacts, 0);
	facts.discard();
	return OK;
}

/**
 * 该函数专门用来解析演示用的数据集，非常dirty的操作
 * @param fileName 原始数据集
 * @param builder builder类实例
 */
void TripleBitBuilder::parserTriplesFile(string fileName, TempFile &rawFacts) {
	std::ifstream in(fileName);
	std::string str;
	//in.open(fileName);
	cout << "TripleBitBuilder::parserTriplesFile" << endl;
	bool flag = true;
	while (getline(in, str)) {
		if (flag) {
		    cout << "master/TripleBitBuilder.cpp:380" << endl;
		    flag = false;
		}
		int pos1 = 0, pos2 = str.find("\t", 0);
		std::string subject = str.substr(pos1, pos2 - pos1);
		pos1 = pos2 + 1;
		pos2 = str.find("\t", pos1);
		std::string predicate = str.substr(pos1, pos2 - pos1);
		pos1 = pos2 + 1;
		int len = str.length();
		std::string object;
		if (str[len - 1] == '\r')
			object = str.substr(pos1, len - pos1 - 1);
		else
			object = str.substr(pos1, len - pos1);
		cout << object << "|" << predicate << "|" << subject << endl;
		if (predicate.size() && subject.size() && object.size())
			NTriplesParse(subject.c_str(), predicate.c_str(), object.c_str(), rawFacts);
	}
	in.close();
}

/**
 * 将原始数据文件映射成ID文件
 * @param dataFile 原始数据文件
 * @param targetDir 分解后的数据要存放的目录
 */
Status TripleBitBuilder::startBuild(const string &dataFile, const string &targetDir, TempFile &rawFacts) {
	std::cout << "TripleBitBuilder::startBuild" << std::endl;
	if (!OSFile::directoryExists(targetDir)) {
		OSFile::mkdir(targetDir);
	}
	
	if (LANG) {
		// Created by peng on 2019-12-15, 16:35:09
		// chinese case
		parserTriplesFile(dataFile, rawFacts);
	} else {
		// Created by peng on 2019-12-15, 16:35:21
		// english case
		
		ifstream in(dataFile.c_str());
		if (!in.is_open()) {
			cerr << "Unable to open " << dataFile << endl;
			return ERROR;
		}
		if (!N3Parse(in, dataFile.c_str(), rawFacts)) {
			in.close();
			return ERROR;
		}
		in.close();
	}
	// Created by peng on 2019-12-15, 16:50:15
	// 将table中的数据写到磁盘上
	delete uriTable;
	uriTable = NULL;
	delete preTable;
	preTable = NULL;
	delete staReifTable;
	staReifTable = NULL;
	
	// Created by peng on 2019-12-15, 16:50:40
	// rawfacts文件中存放的是
	// IDIDID (小端方式，紧挨着存放)
	// IDIDID
	// 的形式，由原始数据文件经过映射生成。
	rawFacts.flush();
	return OK;
}

Status TripleBitBuilder::buildIndex() {
	// build hash index;
	/*MMapBuffer* bitmapIndex;
	cout<<"build hash index for subject"<<endl;
	for ( map<ID,ChunkManager*>::iterator iter = bitmap->predicate_managers[0].begin(); iter != bitmap->predicate_managers[0].end(); iter++ ) {
		if ( iter->second != NULL ) {
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}

	cout<<"build hash index for object"<<endl;
	for ( map<ID, ChunkManager*>::iterator iter = bitmap->predicate_managers[1].begin(); iter != bitmap->predicate_managers[1].end(); iter++ ) {
		if ( iter->second != NULL ) {
			iter->second->buildChunkIndex();
			iter->second->getChunkIndex(1)->save(bitmapIndex);
			iter->second->getChunkIndex(2)->save(bitmapIndex);
		}
	}
*/
	return OK;
}

Status TripleBitBuilder::endBuild() {
	cout << "TripleBitBuilder::endBuild" << endl;
	
	//bitmap->save();
	//ofstream ofile(string(dir + "/statIndex").c_str());
	
	MMapBuffer *indexBuffer[SLAVE_NUM];
	for (int i = 0; i < SLAVE_NUM; ++i) {
		indexBuffer[i] = NULL;
		((OneConstantStatisticsBuffer *) statBuffer[i][0])->save(indexBuffer[i], i);
		((OneConstantStatisticsBuffer *) statBuffer[i][1])->save(indexBuffer[i], i);
		((TwoConstantStatisticsBuffer *) statBuffer[i][2])->save(indexBuffer[i], i);
		((TwoConstantStatisticsBuffer *) statBuffer[i][3])->save(indexBuffer[i], i);
		delete indexBuffer[i];
	}
	return OK;
}
