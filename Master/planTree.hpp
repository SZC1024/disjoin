#ifndef planTree_hpp
#define planTree_hpp

#include <stdio.h>
#include"generalQuery.hpp"
#include<iostream>
#include<map>
#include<deque>
#include<vector>
#include<list>
#include "subQuery.hpp"
using namespace std;

struct TreeNode{
    size_t id;//如果是虚结点的话，id是0
    TreeNode* parent;
    TreeNode* left;
    TreeNode* right;
    int type;//表示是join得来的还是union得来的，0：原始子查询，1：union，2：join，-1：表示为了生成完全二叉树而填充的虚结点
    int weight;
    TreeNode() {
        left = NULL;
        right = NULL;
        parent = NULL;
    }
};

class PlanTree
{
private:
    /* data */
public:
    TreeNode* root;
    map<size_t, subQuery*> idtosubq;
    vector<TreeNode*> emptyNode; //空结点池，用来任务结束时delete所有空结点
    map<size_t, TreeNode*> idtonode;//连接计划树实节点
    PlanTree();

    //根据给定参数map生成计划树，map tree里第一个参数是结点的id，第二个参数是该节点的父节点id，root的父节点为0
    PlanTree(map<size_t,size_t>* tree,map<size_t,subQuery*> idtosubq);

    //将node所指的普通二叉树转换为完全二叉树，通过添加空属性节点做到
    void toCompleteBinaryTree(TreeNode* node);

    //将current指向的树填充成深度为deep的完全二叉树，current传入必不能为null
    void fillNode(TreeNode* t, int deep);

    //将node所指的完全二叉树转换为数组存储的形式，通过BFS做到
    vector<TreeNode*>* completeBinaryTreeToVector(TreeNode* node);

    //将当前计划树分解为指定（planTreeNum）个数
    vector<PlanTree*>* decomposePlanTree(int planTreeNum);

    //在当前的PlanTree的基础上，将以node为根的子树复制一份作为PlanTree返回
    PlanTree* copyChildPlanTree(TreeNode* node);

    ~PlanTree(){
        for (int i = 0; i < emptyNode.size();i++) {
            delete emptyNode[i];
        }
        for (int i = 0; i < idtonode.size();i++) {
            delete idtonode[i];
        }
        idtosubq.clear();
    }
};
#endif /* planTree_hpp */
