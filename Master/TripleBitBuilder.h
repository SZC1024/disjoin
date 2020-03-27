#ifndef TRIPLEBITBUILDER_H_
#define TRIPLEBITBUILDER_H_

#define TRIPLEBITBUILDER_DEBUG 0

class PredicateTable;

class URITable;

class URIStatisticsBuffer;

class StatementReificationTable;

class FindColumns;

// class BitmapBuffer;

class Sorter;

class TempFile;

class StatisticsBuffer;

#include <fstream>
#include <pthread.h>
#include <cassert>
#include <cstring>
#include <vector>

#include "TripleBit.h"
#include "StatisticsBuffer.h"
#include "TurtleParser.hpp"
#include "TempFile.h"
#include "OSFile.h"

using namespace std;

class TripleBitBuilder {
private:
	// Created by peng on 2019-12-15, 22:13:57
	// 主节点上不需要使用
	//BitmapBuffer *bitmap;
	PredicateTable *preTable;
	URITable *uriTable;
	vector <string> predicates;
	string dir;
	/// statistics buffer;
	StatisticsBuffer **statBuffer[SLAVE_NUM];
	StatementReificationTable *staReifTable;
	FindColumns *columnFinder;
public:
	TripleBitBuilder();
	
	TripleBitBuilder(const string dir);
	
	Status initBuild();
	
	Status startBuild(const string &dataFile, const string &targetDir, TempFile &rawFacts);
	
	void parserTriplesFile(string fileName, TempFile &rawFacts);
	
	static const char *skipIdIdId(const char *reader);
	
	static int compareValue(const char *left, const char *right);
	
	static int compare213(const char *left, const char *right);
	
	static int compare231(const char *left, const char *right);
	
	static int compare123(const char *left, const char *right);
	
	static int compare321(const char *left, const char *right);
	
	static inline void loadTriple(const char *data, ID &v1, ID &v2, ID &v3) {
		TempFile::readId(TempFile::readId(TempFile::readId(data, v1), v2), v3);
	}
	
	static inline int cmpValue(ID l, ID r) {
		return (l < r) ? -1 : ((l > r) ? 1 : 0);
	}
	
	static inline int cmpTriples(ID l1, ID l2, ID l3, ID r1, ID r2, ID r3) {
		int c = cmpValue(l1, r1);
		if (c)
			return c;
		c = cmpValue(l2, r2);
		if (c)
			return c;
		return cmpValue(l3, r3);
		
	}
	
	StatisticsBuffer *getStatBuffer(StatisticsBuffer::StatisticsType type, int slaveID) {
		return statBuffer[slaveID][static_cast<int>(type)];
	}
	
	Status resolveTriples(TempFile &rawFacts, int slaveID);
	
	Status startBuildN3(string fileName);
	
	bool N3Parse(istream &in, const char *name, TempFile &);
	
	Status importFromMySQL(string db, string server, string username, string password);
	
	void NTriplesParse(const char *subject, const char *predicate, const char *object, TempFile &rawFacts);
	
	bool generateXY(ID &subjectID, ID &objectID);
	
	Status buildIndex();
	
	Status endBuild();
	
	static bool isStatementReification(const char *object);
	
	virtual ~TripleBitBuilder();
};

#endif /* TRIPLEBITBUILDER_H_ */
