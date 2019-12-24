//
//  planTree.cpp
//  Master
//
//  Created by 周华健 on 2019/12/23.
//  Copyright © 2019 周华健. All rights reserved.
//
#include "planTree.hpp"

PlanTree::PlanTree(){
    root=NULL;
}

//根据给定参数map生成计划树，map tree里第一个参数是结点的id，第二个参数是该节点的父节点id，root的父节点为0
PlanTree::PlanTree(map<size_t,size_t>* tree, map<size_t, subQuery*> idtosubq){
    for(map<size_t,size_t>::reverse_iterator iter=tree->rbegin();iter!=tree->rend();iter++){
        //先遍历一遍，生成id，parent和type信息  (weight信息暂不使用)
	cout<<"11111"<<endl;
        if (idtonode.find(iter->first)==idtonode.end()) {
            idtonode[iter->first] = new TreeNode();
            idtonode[iter->first]->id = iter->first;
            if(iter->second!=0) idtonode[iter->first]->parent = idtonode[iter->second];
            if(iter->second==0) root = idtonode[iter->first];
            idtonode[iter->first]->type = idtosubq[iter->first]->getType();
        }
    }
    for (map<size_t, TreeNode*>::iterator iter = idtonode.begin(); iter != idtonode.end();iter++) {
        //再遍历一遍，生成该节点的父节点的left和right信息
	cout<<"222222222"<<endl;
        if ( iter->second!=root ) {
            if (iter->second->parent->left==NULL) {
                iter->second->parent->left = iter->second;
            }else {
                iter->second->parent->right = iter->second;
            }
        }
    }
    //idtonode里存储了整个树
    root = idtonode[tree->size()];
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

//将当前计划树分解为指定（planTreeNum）个数
vector<PlanTree*>* PlanTree::decomposePlanTree(int planTreeNum) {
    vector<PlanTree*>* smallTree = new vector<PlanTree*>();
    smallTree->push_back(this);
    
    return smallTree;
}
