package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.util.TypeHandler.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.csu.mydb.storage.PageManager.*;

public class LeafNode<K extends Comparable<K>> extends BPlusNode<K> {
    protected GlobalPageId values; // 数据页指针列表（缓存）
    protected LeafNode<K> next; // 叶子节点链表指针
    protected LeafNode<K> prev; // 叶子节点链表指针
    protected InternalNode<K> parent;

    public LeafNode(TypeHandler<K> keyHandler) {
        super();
        this.keyHandler = keyHandler;
        this.isLeaf = true;
        this.gid = new GlobalPageId(-1, -1); // 初始化为无效页号（需分配）
        this.header = new PageHeader();
        this.header.pageType = 1; // 索引页类型（B+树索引属于索引页）
        this.header.slotCount = 0;
        this.header.freeSpace = (short) (PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    @Override
    protected void deserializeKeys(byte[] pageData) {
        this.keys = new ArrayList<>();
        this.values = gid;
        int dataStart = PAGE_HEADER_SIZE + header.slotCount * SLOT_SIZE;
        for (Slot slot : slots) {
            if (slot.status != 1) continue; // 跳过空闲槽位
            int keyLen = slot.length - 8; // 总长度 - 数据页指针长度（8字节）
            byte[] keyBytes = new byte[keyLen];
            // 从页数据中提取键（假设页数据已加载到内存，需补充缓存）
            System.arraycopy(pageData, dataStart + slot.offset + 8, keyBytes, 0, keyLen);
            K key = keyHandler.deserialize(ByteBuffer.wrap(keyBytes)); // TypeHandler 反序列化键
            this.keys.add(key);
            // 提取数据页指针（假设存储为 spaceId(4字节) + pageNo(4字节)）
            int spaceId = ByteBuffer.wrap(pageData, dataStart + slot.offset, 4).getInt();
            int pageNo = ByteBuffer.wrap(pageData, dataStart + slot.offset + 4, 4).getInt();
            this.values.add(new GlobalPageId(spaceId, pageNo));
        }
    }

    @Override
    protected byte[] serializeToPage() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        // 写入页头
        buffer.put(header.toBytes());
        // 写入槽位和键值对数据

        return buffer.array();
    }

    // 插入键值对（键 + 数据页指针）
    public void insert(K key, GlobalPageId value, int order) throws IOException {
        int idx = findKeyIndex(key);



        // 检查是否溢出（超过阶数限制）
        if (isOverflow(order)) {
            split(order);
        }
        save(); // 保存修改到磁盘
    }

    // 分裂叶子节点（需实现）
    public void split(int order) throws IOException {
        // 1. 计算分裂位置（中间位置）
        int mid = keys.size() / 2;
        // 2. 创建新叶子节点
        LeafNode<K> newLeaf = new LeafNode<>(keyHandler);
        newLeaf.keys.addAll(keys.subList(mid, keys.size()));
        newLeaf.values.addAll(values.subList(mid, keys.size()));
        // 3. 更新原节点
        keys.subList(mid, keys.size()).clear();
        values.subList(mid, keys.size()).clear();
        // 4. 维护叶子链表
        newLeaf.next = this.next;
        newLeaf.prev = this;
        this.next = newLeaf;
        if (newLeaf.next != null) newLeaf.next.prev = newLeaf;

        // 5. 保存新节点到磁盘
        newLeaf.gid = PageManager.allocateNewPageId(); // 分配新页号（需实现）

        // 6. 更新父节点（内部节点）的键
        if (parent == null) {
            // 原节点是根，创建新根（内部节点）
            InternalNode<K> newRoot = new InternalNode<>(keyHandler);
            newRoot.keys.add(newLeaf.keys.get(0));
            newRoot.children.add(gid);
            newRoot.children.add(newLeaf.gid);
            parent = newRoot;
            newRoot.parent = null;
        } else {
            // 将新叶子的最小键插入父节点
            parent.insertKey(newLeaf.keys.get(0), newLeaf.gid, order);
        }
        newLeaf.save();
    }

    // 溢出
    public boolean isOverflow(int order) {
        return keys.size() > order;
    }
}
