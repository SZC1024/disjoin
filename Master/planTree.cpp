#include "planTree.hpp"

const int _debug_for_szc_ = 1;

PlanTree::PlanTree(){
    root=NULL;
}

//根据给定参数map生成计划树，map tree里第一个参数是结点的id，第二个参数是该节点的父节点id，root的父节点为0
PlanTree::PlanTree(map<size_t,size_t>* tree, map<size_t, subQuery*> idtosubq){
    this->idtosubq = idtosubq;
    for(map<size_t,size_t>::reverse_iterator iter=tree->rbegin();iter!=tree->rend();iter++){
        //先遍历一遍tree，生成id，parent和type信息  (weight信息暂不使用)
        if (idtonode.find(iter->first)==idtonode.end()) {
            idtonode[iter->first] = new TreeNode();
            idtonode[iter->first]->id = iter->first;
            if(iter->second!=0) idtonode[iter->first]->parent = idtonode[iter->second];
            if(iter->second==0) root = idtonode[iter->first];
            idtonode[iter->first]->type = idtosubq[iter->first]->getType();
        }
    }
    for (map<size_t, TreeNode*>::iterator iter = idtonode.begin(); iter != idtonode.end();iter++) {
        //再遍历一遍idtonode，生成该节点的父节点的left和right信息
        if ( iter->second!=root ) {
            if (iter->second->parent->left==NULL) {
                iter->second->parent->left = iter->second;
            }else {
                iter->second->parent->right = iter->second;
            }
        }
    }
    //idtonode里存储了整个树
    //root = idtonode[tree->size()];//其实这句话可以不要，因为上边赋值过了
}

//得到树根为t的树的深度
int getDeep(TreeNode* t) {
    if (t == NULL) return 0;
    return max(getDeep(t->left)+1, getDeep(t->right)+1);
}

//将current指向的树填充成深度为deep的完全二叉树，current传入必不能为null
void PlanTree::fillNode(TreeNode* current,int deep) {
    if (deep <= 1) return;
    else {
        if (current->left == NULL) {
            TreeNode* temp = new TreeNode();
            emptyNode.push_back(temp);
            temp->type = -1;
            temp->parent = current;
            temp->id = 0;
            current->left = temp;
            fillNode(temp, deep - 1);
        }else fillNode(current->left, deep - 1);

        if (current->right == NULL) {
            TreeNode* temp = new TreeNode();
            emptyNode.push_back(temp);
            temp->type = -1;
            temp->parent = current;
            temp->id = 0;
            current->right = temp;
            fillNode(temp, deep - 1);
        }else fillNode(current->right, deep - 1);
        
    }
}

//将node所指的普通二叉树转换为完全二叉树，通过添加空属性节点做到
void PlanTree::toCompleteBinaryTree(TreeNode* node) {
    //具体通过扫描得到树的最深深度
    //之后通过dfs添加空属性节点
    //需要一个空结点池emptyNode，用来任务结束时delete所有空结点
    int maxdeep = getDeep(node);
    fillNode(node,maxdeep);
    //fillNode之后的node为指向完全（满）二叉树

}


//将node所指的完全二叉树转换为数组存储的形式，通过BFS做到
vector<TreeNode*>* PlanTree::completeBinaryTreeToVector(TreeNode* node) {
    //bfs
    vector<TreeNode*>* vectorStoreNode=new vector<TreeNode*>();//需要返回给上层
    deque<TreeNode*> bfsDeque;
    bfsDeque.push_back(node);
    while (!bfsDeque.empty()){
        TreeNode* current = bfsDeque.front();
        vectorStoreNode->push_back(current);
        if (current->left != NULL) bfsDeque.push_back(current->left);
        if (current->right != NULL) bfsDeque.push_back(current->right);
        bfsDeque.pop_front();
    }
    return vectorStoreNode;
}

//decomposePlanTree调用，以dfs递归的方式搜索root为根的树中所有第一代父节点，传参必须保证root度为2
void searchFirstGenerationParent(TreeNode* node,list<TreeNode*>& firstGenerationParent){
    //首先可以肯定的是PlanTree中没有度为1的节点
    if ((node->type == 2) && (node->left->type != 2 || node->right->type != 2)) {
        firstGenerationParent.push_back(node);
    }else{
        if (node->left->type == 2) searchFirstGenerationParent(node->left, firstGenerationParent);
        if (node->right->type == 2) searchFirstGenerationParent(node->right, firstGenerationParent);
    }
}

//将node所指的子树转换为tree map，tree中第一个参数为该节点id，第二个参数为该节点父亲id
void getTreeMap(TreeNode* node, map<size_t, size_t>* tree, int deep = 1) {
	if (deep == 1) (*tree)[node->id] = 0;//根节点，tree中根节点parent为0，保证tree的结构和generalQuery的createPlan中tree一样，方便直接调用PlanTree构造函数
	else (*tree)[node->id] = node->parent->id;
	if (node->left != NULL) getTreeMap(node->left, tree, deep + 1);
	if (node->right != NULL) getTreeMap(node->right, tree, deep + 1);
}

//将当前计划树（root指定为准）分解为指定（planTreeNum）个数
vector<PlanTree*>* PlanTree::decomposePlanTree(int planTreeNum) {
    vector<PlanTree*>* smallTree = new vector<PlanTree*>();//用来存最终结果
    
 //   //不分解，调试专用代码段
	//smallTree->push_back(this);//之后直接将原树存进smallTree中即可
	//return smallTree;

    //先找出PlanTree中的第一代父节点，用链表存储
    list<TreeNode*> selectedParentNode;
    if(root->type != 2){
        //如果PlanTree中压根没有join操作
        selectedParentNode.push_back(root);
    }else{
        searchFirstGenerationParent(root, selectedParentNode);
    }

    if (_debug_for_szc_) {
        cout << "当前join树的第一代父节点为:";
        for(auto a:selectedParentNode){
            cout << " " << a->id;
        }
        cout << endl;
    }

    //接下来合并selectedParentNode中的节点
    cout << "开始合并相邻节点" << endl;
    while(selectedParentNode.size() > planTreeNum){//首先逐个合并相邻的节点（其中一个为另一个父亲）
        int flag = 0;
        for (list<TreeNode*>::iterator i = selectedParentNode.begin(); i != selectedParentNode.end();i++) {
            list<TreeNode*>::iterator j = i;
            j++;
            for (; j != selectedParentNode.end(); j++) {
                if((*i)->parent == *j){
                    selectedParentNode.erase(i);
                    TreeNode* temp = *j;
                    selectedParentNode.erase(j);
                    selectedParentNode.push_back(temp);//把他放置在链表末尾，避免合并聚集
                    flag = 1;
                    break;
                }
                if((*j)->parent == *i){
                    selectedParentNode.erase(j);
                    TreeNode* temp = *i;
                    selectedParentNode.erase(i);
                    selectedParentNode.push_back(temp);//同上
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) break;
        }
        if (flag == 0) break;//搜了一圈发现flag还是0，说明没有没有相邻节点，直接退出循环
    }
    cout << "开始合并具有共同父亲的节点" << endl;
    while(selectedParentNode.size() > planTreeNum){//接下来逐个合并具有共同父亲的节点
        int flag = 0;
        for (list<TreeNode*>::iterator i = selectedParentNode.begin(); i != selectedParentNode.end();i++) {
            list<TreeNode*>::iterator j = i;
            j++;
            for (; j != selectedParentNode.end();j++) {
                if((*i)->parent == (*j)->parent){
                    selectedParentNode.push_back((*i)->parent);
                    selectedParentNode.erase(i);
                    selectedParentNode.erase(j);
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) break;
        }
        //这里必然不会死循环，因为不存在没有共同节点的第一代父节点，直到合并的只剩下root
    }
    //划分完毕，接下来就是将每个划分都完整的表示成连接计划树
    cout << "节点合并完毕，开始选择root的归属" << endl;
    //首先找到最矮树，需要将总连接计划树其余任务划分给它
    TreeNode* selectedRoot;
    int selectedRootDeep = 99999999;
    for (list<TreeNode*>::iterator iter = selectedParentNode.begin(); iter != selectedParentNode.end();iter++) {
        int temp = getDeep(*iter);
        if(temp < selectedRootDeep){
            selectedRootDeep = temp;
            selectedRoot = *iter;
        }
    }
    cout << "选择完毕，开始复制PlanTree副本" << endl;
    //接下来复制子树，除了最矮树
    for (list<TreeNode*>::iterator iter = selectedParentNode.begin(); iter != selectedParentNode.end();iter++) {
        if(*iter != selectedRoot){
            map<size_t, size_t>* tree = new map<size_t, size_t>();
            getTreeMap(*iter, tree);//根据iter所指treenode，填充tree map
            if(_debug_for_szc_){
				cout << "idtosubq:<id,subquery-type> =";
				for (map<size_t, subQuery*>::iterator i = idtosubq.begin(); i != idtosubq.end(); i++) {
					cout << " <" << i->first << "," << i->second->getType() << ">";
				}
				cout << endl;
				cout << "tree:<id,parent-id> =";
				for (map<size_t, size_t>::iterator i = tree->begin(); i != tree->end(); i++) {
					cout << " <" << i->first << "," << i->second << ">";
				}
				cout << endl;
            }
            PlanTree* plantree = new PlanTree(tree, idtosubq);//这里只是一个copy过程，不涉及更改原树结构
            smallTree->push_back(plantree);
        }
    }
    //接下来处理总树和最矮树
    for(list<TreeNode*>::iterator iter = selectedParentNode.begin(); iter != selectedParentNode.end(); iter++){
        if(*iter != selectedRoot){
            //原树中删除掉除了最矮树的其他子树
            (*iter)->left = NULL;
            (*iter)->right = NULL;
        }
    }
    smallTree->push_back(this);//之后直接将原树存进smallTree中即可
    return smallTree;
}



////copyChildPlanTree调用，以dfs递归的方式复制
//void dfsCopy(TreeNode* oldtree,TreeNode* newtree){
//
//    newtree->id = oldtree->id;
//    newtree->type = oldtree->type;
//    newtree->weight = oldtree->weight;
//
//
//    TreeNode* node = new TreeNode();
//    node->id = oldtree->id;
//    node->parent = newtree
//
//    if(oldtree->left!=NULL){
//        dfsCopy(oldtree->left,)
//    }
//
//
//}
//
////在当前的PlanTree的基础上（无虚结点），将以node为根的子树复制一份作为PlanTree返回
//PlanTree* PlanTree::copyChildPlanTree(TreeNode* node){
//
//}