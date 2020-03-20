# disjoin-Master
 disjoin完整版

Slave文件夹放在slave节点的triplebit根目录中，更改triplebit的makefile，使其和triplebit一起编译

DataDec和其附属文件夹（subData）是数据分解部分，单独编译，单独使用，编译命令g++ -std=c++0x  -Wall -g -O0 -std=c++0x -I /home/ganpeng/boost_1_72_0 -o bin/lrelease/master ./master/*.cpp -lpthread（来组ganpeng）

Master和其附属的文件夹（Query，subQuery）是分布式部分的Master部分，单独编译，单独使用，使用时和slave相互通讯合作