package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.bufferPool.BufferPool;
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
    protected PageManager pageManager;
    protected BufferPool bufferPool;
    protected StorageSystem storageSystem;

    protected static final Logger logger = LoggerFactory.getLogger(BPlusNode.class);

    // 父节点引用
    public InternalNode parent;

    public BPlusNode(PageManager.GlobalPageId gid, PageManager.PageHeader header, boolean b, StorageSystem storageSystem) {
        this.gid = gid;
        this.header = header;
        this.isLeaf = b;
        this.pageManager = storageSystem.getPageManager();
        this.bufferPool = storageSystem.getBufferPool();
        this.storageSystem = storageSystem;
        keys = Collections.synchronizedList(new ArrayList<>());
    }

    // 查找键对应的位置（二分查找）
    protected int findKeyIndex(Key key) {
        return Collections.binarySearch(keys, key);
    }

}
