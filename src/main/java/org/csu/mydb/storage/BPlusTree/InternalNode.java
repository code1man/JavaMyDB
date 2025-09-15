package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.util.TypeHandler.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.csu.mydb.storage.PageManager.*;

public class InternalNode<K extends Comparable<K>> extends BPlusNode<K> {
    protected List<GlobalPageId> children; // 子节点指针列表（缓存）
    protected InternalNode<K> parent; // 父节点指针

    public InternalNode(TypeHandler<K> keyHandler) {
        this.keyHandler = keyHandler;
        this.isLeaf = false;
        this.gid = new GlobalPageId(-1, -1); // 初始化为无效页号（需分配）
        this.header = new PageHeader();
        this.header.pageType = 1; // 索引页类型
        this.header.slotCount = 0;
        this.header.freeSpace = (short) (PAGE_SIZE - PAGE_HEADER_SIZE);
        this.children = new ArrayList<>();
    }

    @Override
    protected void deserializeKeys(byte[] pageData) {
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        int dataStart = PAGE_HEADER_SIZE + header.slotCount * SLOT_SIZE;
        for (Slot slot : slots) {
            if (slot.status != 1) continue; // 跳过空闲槽位
            int keyLen = slot.length - 8; // 总长度 - 子节点指针长度（8字节）
            byte[] keyBytes = new byte[keyLen];
            // 从页数据中提取键
            System.arraycopy(pageData, dataStart + slot.offset + 8, keyBytes, 0, keyLen);
            K key = keyHandler.deserialize(ByteBuffer.wrap(keyBytes)); // TypeHandler 反序列化键
            this.keys.add(key);
            // 提取子节点指针
            int childSpaceId = ByteBuffer.wrap(pageData, dataStart + slot.offset, 4).getInt();
            int childPageNo = ByteBuffer.wrap(pageData, dataStart + slot.offset + 4, 4).getInt();
            this.children.add(new PageManager.GlobalPageId(childSpaceId, childPageNo));
        }
    }

    @Override
    protected byte[] serializeToPage() {
        ByteBuffer buffer = ByteBuffer.allocate(PAGE_SIZE);
        // 写入页头
        buffer.put(header.toBytes());
        // 写入槽位和键值对数据
        int dataOffset = PAGE_HEADER_SIZE;
        for (int i = 0; i < keys.size(); i++) {
            K key = keys.get(i);
            GlobalPageId child = children.get(i + 1); // 子节点列表比键多一个（第一个子节点是最左）
            // 序列化键
            byte[] keyBytes = keyHandler.serialize(key); // TypeHandler 序列化键
            int keyLen = keyBytes.length;
            // 序列化子节点指针（spaceId + pageNo）
            byte[] gidBytes = new byte[8];
            ByteBuffer gidBuffer = ByteBuffer.wrap(gidBytes);
            gidBuffer.putInt(child.spaceId);
            gidBuffer.putInt(child.pageNo);
            // 计算槽位偏移量（当前数据区的起始位置）
            int slotOffset = dataOffset + keyLen + 8;
            // 写入槽位（offset=slotOffset, length=keyLen+8, status=1）
            Slot slot = new Slot();
            slot.offset = (short) slotOffset;
            slot.length = (short) (keyLen + 8);
            slot.status = 1;
            slot.nextFree = -1;
            buffer.put(slot.toBytes(), dataOffset - SLOT_SIZE, SLOT_SIZE); // 槽位数组在页头之后
            // 写入键和子节点指针到数据区
            buffer.put(keyBytes, dataOffset, keyLen);
            buffer.put(gidBytes, dataOffset + keyLen, 8);
            dataOffset += keyLen + 8;
        }
        // 更新页头的空闲空间和记录数
        header.slotCount = (short) keys.size();
        header.recordCount = header.slotCount;
        header.freeSpace = (short) (PAGE_SIZE - dataOffset);
        return buffer.array();
    }

    // 插入键和子节点指针
    public void insertKey(K key, GlobalPageId child, int order) throws IOException {
        int idx = findKeyIndex(key);
        if (idx >= 0) {
            // 键已存在，替换子节点（理论上不会发生，因 B+ 树键唯一）
            children.set(idx + 1, child);
        } else {
            int insertPos = -idx - 1;
            keys.add(insertPos, key);
            children.add(insertPos + 1, child);
        }
        // 检查是否溢出（超过阶数限制）
        if (isOverflow(order)) { // 内部节点阶数限制为 order-1（子节点数最多 order）
            split(order);
        }
        save(); // 保存修改到磁盘
    }

    // 分裂内部节点（需实现）
    public void split(int order) throws IOException {
        // 1. 计算分裂位置（中间位置）
        int mid = keys.size() / 2;
        K upKey = keys.get(mid); // 提升到父节点的键
        // 2. 创建新内部节点
        InternalNode<K> newInternal = new InternalNode<>(keyHandler);
        newInternal.keys.addAll(keys.subList(mid + 1, keys.size()));
        newInternal.children.addAll(children.subList(mid + 2, children.size())); // 子节点比键多一个
        // 3. 更新原节点
        keys.subList(mid, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();
        // 4. 更新父节点
        if (parent == null) {
            // 原节点是根，创建新根（内部节点）
            InternalNode<K> newRoot = new InternalNode<>(keyHandler);
            newRoot.keys.add(upKey);
            newRoot.children.add(this);
            newRoot.children.add(newInternal);
            parent = newRoot;
            newRoot.parent = null;
        } else {
            // 将提升的键和 newInternal 插入父节点
            parent.insertKey(upKey, newInternal.gid, order);
        }
        // 5. 保存新节点到磁盘
        newInternal.gid = PageManager.allocateNewPageId(); // 分配新页号（需实现）
        newInternal.save();
    }

    // 溢出
    public boolean isOverflow(int order) {
        return keys.size() > order - 1;
    }
}
