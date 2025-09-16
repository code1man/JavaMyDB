package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// B+树节点抽象类
public abstract class BPlusNode<Key extends Comparable<Key>> {
    public PageManager.GlobalPageId gid; // 该节点对应的磁盘页号
    public PageManager.PageHeader header; // 页头（缓存，避免重复反序列化）
    public List<Key> keys; // 键列表
    public boolean isLeaf;
    protected PageManager pageManager = new PageManager();

    protected static final Logger logger = LoggerFactory.getLogger(BPlusNode.class);

    // 父节点引用
    public InternalNode parent;

    public BPlusNode(PageManager.GlobalPageId gid, PageManager.PageHeader header, boolean b, PageManager pageManager) {
        this.gid = gid;
        this.header = header;
        this.isLeaf = b;
        this.pageManager = pageManager;
        keys = Collections.synchronizedList(new ArrayList<Key>());
    }

    // 查找键对应的位置（二分查找）
    protected int findKeyIndex(Key key) {
        return Collections.binarySearch(keys, key);
    }
}
