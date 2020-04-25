disjoin完整版

Slave文件夹放在slave节点的triplebit根目录中，更改triplebit的makefile，使其和triplebit一起编译

DataDec和其附属文件夹（subData文件夹必备）是数据分解部分，单独编译，单独使用，编译命令g++ -std=c++0x  -Wall -g -O2 -std=c++0x -I /usr/lib/boost_1_72_0 -o ./datadec ./*.cpp -lpthread（来组ganpeng）

Master和其附属的文件夹（Query、subQuery文件夹必备）是分布式部分的Master部分，单独编译，单独使用，使用时和slave相互通讯合作，编译命令g++ -std=c++0x  -Wall -g -O0 -std=c++0x -pthread -I /usr/lib/boost_1_72_0 -o ./master ./*.cpp（来自zhouhuajian）

master下的host文件，第一行0代表0号对应master的ip，master必须是0
slave下的host文件，第一行代表自己是第几号（下边的ip），0号必然是master

当存储与计算分离时，存储节点在host内的编号从100001开始