package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.util.TypeHandler.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.csu.mydb.storage.PageManager.PAGE_HEADER_SIZE;
import static org.csu.mydb.storage.PageManager.SLOT_SIZE;

// B+树节点抽象类
public abstract class BPlusNode<K extends Comparable<K>> {
    protected PageManager.GlobalPageId gid; // 该节点对应的磁盘页号
    protected PageManager.PageHeader header; // 页头（缓存，避免重复反序列化）
    protected List<PageManager.Slot> slots; // 槽位列表（缓存）
    protected List<K> keys; // 键列表
    protected boolean isLeaf;
    protected TypeHandler<K> keyHandler;
    protected PageManager pageManager = new PageManager();

    // 父节点引用（统一放这里）
    protected InternalNode<K> parent;

    // 从磁盘加载页数据到内存（缓存）
    public void load() throws IOException {
        if (gid == null) throw new IllegalStateException("GlobalPageId not initialized");
        PageManager.Page pageData = pageManager.readPageFromDisk(gid.spaceId, gid.pageNo); // 调用 PageManager 读取页
        this.header = pageData.getHeader();
        this.slots = new ArrayList<>();
        int slotsStart = PAGE_HEADER_SIZE;
        int slotsEnd = slotsStart + header.slotCount * SLOT_SIZE;
        ByteBuffer buffer = ByteBuffer.wrap(pageData.getPageData(), slotsStart, slotsEnd - slotsStart);
        for (int i = 0; i < header.slotCount; i++) {
            slots.add(PageManager.Slot.fromBytes(buffer.array())); // 反序列化槽位
        }
        this.keys = new ArrayList<>();
        deserializeKeys(pageData.getPageData()); // 子类实现：从数据区解析键
    }

    // 将内存中的节点数据保存到磁盘（更新页）
    public void save() throws IOException {
        byte[] pageData = serializeToPage(); // 序列化节点到页数据

        pageManager.writePageToDisk(gid.spaceId, gid.pageNo, PageManager.Page.fromBytes(pageData)); // 调用 PageManager 写入页
        this.header.isDirty = false; // 标记页为干净
    }

    // 抽象方法：从数据区解析键（子类实现）
    protected abstract void deserializeKeys(byte[] pageData);

    // 抽象方法：序列化键和值到数据区（子类实现）
    protected abstract byte[] serializeToPage();

    // 查找键对应的位置（二分查找）
    protected int findKeyIndex(K key) {
        return Collections.binarySearch(keys, key);
    }
}
