//
//  manageToMaster.hpp
//  Master
//
//  Created by 周华健 on 2019/12/18.
//  Copyright © 2019 周华健. All rights reserved.
//  管理节点

#ifndef manageToMaster_hpp
#define manageToMaster_hpp

#include "generalQuery.hpp"
#include "partitionToQuery.hpp"
#include "subQuery.hpp"
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <string.h>
#include <algorithm>
#include <fstream>

using namespace std;

class manageToMaster{
    
private:
    size_t ID;
    unordered_map<size_t, generalQuery* > queryRef;
    unordered_map<size_t, string> ipRef;
public:
    static size_t queryId;
    manageToMaster();
    size_t addQuery(string str);
    bool removeQuery(size_t id1);
    unordered_map<size_t, string> getIpRef();
    size_t getID() const;
    vector<vector<size_t> > exeuteQuery(size_t id);
    size_t getQuerySize() const;
    size_t getHostNum() const;
};



#endif /* manageToMaster_hpp */
