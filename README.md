disjoin完整版

部署步骤（for-use分支）：

1）所有节点安装boost，版本大于1.71.0即可。安装目录为/usr/lib/boost_*_*_*，其中版本号由安装版本号决定。
	master节点还需安装jdk，版本大于1.8即可。

2）将grace-slave在所有slave节点放一份，路径随意。配置host文件，使其对应每个slave和master节点的ip。
	grace-slave：https://github.com/dis-triplebit/grace-slave/tree/rawStatisticsBuffer

3）将disjoin中的Slave文件夹放在slave节点的grace-slave根目录中，更改grace-slave的makefile，使其和grace-slave一起编译，直接make。

4）disjoin中Slave文件夹无需在master节点中出现，因此可以删去。接下来将剩余所有文件放在master节点中，根文件夹命名随意。并配置host文件，使其对应每个slave和master节点的ip。

5）分别编译master节点下的DataDec和Master文件夹。

使用步骤（for-use分支）：

1）使用datadec将原始数据（例如Universities.nt）进行数据分解（subData文件夹必备且为空）。

2）将subData中的分解之后的slave后缀文件分别放在每个slave节点grace-slave文件夹下。

3）所有slave节点进行数据导入工作，命令bin/lrelease/buildTripleBitFromN3。参数中的数据库目录需设定为mydatabase。

4）启动slave节点，命令为bin/lrelease/startslave。

5）启动master节点，命令为Master/master，接下来根据提示输入查询语句文件名（Query、subQuery、subData文件夹必备，其中查询语句文件在Query文件夹中）。

★DataDec文件夹应用范围及编译要求：
DataDec和其附属文件夹（subData文件夹必备）是数据分解部分，单独编译，单独使用，编译命令g++ -std=c++0x  -Wall -g -O2 -std=c++0x -I /usr/lib/boost_1_72_0 -o ./datadec ./*.cpp -lpthread（其中boost_1_72_0改为当前使用的boost版本号）

★Master文件夹编译要求：
Master和其附属的文件夹（Query、subQuery、subData文件夹必备）是分布式部分的Master部分，单独编译，单独使用，使用时和slave相互通讯合作，编译命令g++ -std=c++0x  -Wall -g -O0 -std=c++0x -pthread -I /usr/lib/boost_1_72_0 -o ./master ./*.cpp（其中boost_1_72_0改为当前使用的boost版本号）

★host文件规则：
master下的host文件，第一行0代表0号对应master的ip，master必须是0
slave下的host文件，第一行代表自己是第几号（下边的ip），0号必然是master
当存储与计算分离时（测试专用，正式使用时无需考虑），存储节点在host内的编号从100001开始