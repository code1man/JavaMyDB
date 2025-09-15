package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.PageManager.*;
import org.csu.mydb.util.TypeHandler.TypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

public class BPlusTree<K extends Comparable<K>> {
    private final int order; // B+ 树的阶数（决定节点最大键数）
    private final TypeHandler<K> keyHandler;
    private final BPlusNode<K> root; // 根节点

    private static final Logger logger = LoggerFactory.getLogger(BPlusTree.class);

    public BPlusTree(int order, TypeHandler<K> keyHandler, int spaceId) throws IOException {
        this.order = Math.max(3, order); // 最小阶数保护
        this.keyHandler = keyHandler;
        this.root = new LeafNode<>(keyHandler, 3); // 我想要的效果是根节点永远是page3，其他的可以改
        // 初始化根节点的页号
        root.gid = new GlobalPageId(spaceId, 3);
        root.load(); // 加载页（此时页为空）
        root.save(); // 保存初始空页
    }

    // 查找键对应的数据页指针（叶子节点中的值）
    public GlobalPageId search(K key) throws IOException {
        LeafNode<K> leaf = findLeaf(key);
        leaf.load(); // 加载页到缓存
        int idx = Collections.binarySearch(leaf.keys, key);
        return idx >= 0 ? leaf.gid : null;
    }

    // 查找键所在的叶子节点
    private LeafNode<K> findLeaf(K key) throws IOException {
        BPlusNode<K> node = root;
        while (!node.isLeaf) { // 这里需要修正：root 可能是内部节点（当树高度>1时）
            InternalNode<K> internal = null;
            if (node instanceof InternalNode<K>) {
                internal = (InternalNode<K>) node;
            }
            int idx = Collections.binarySearch(internal.keys, key);
            int childIdx = idx >= 0 ? idx + 1 : -idx - 1;
            PageManager pageManager = new PageManager();
            GlobalPageId childId = internal.children.get(childIdx);
            node.deserializeKeys(pageManager.readPageFromDisk(childId.spaceId, childId.pageNo).getPageData()); // 加载子节点页
        }
        return (LeafNode<K>) node;
    }

    // 插入键值对（键 + 数据页指针）
    public void insert(K key, byte[] records ,GlobalPageId value) throws IOException {
        LeafNode<K> leaf = findLeaf(key);
        leaf.insert(key, records, order); // 叶子节点插入
    }
}