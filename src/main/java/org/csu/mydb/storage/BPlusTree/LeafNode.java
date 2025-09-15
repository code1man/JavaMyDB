package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.util.TypeHandler.TypeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static org.csu.mydb.storage.PageManager.*;

public class LeafNode<K extends Comparable<K>> extends BPlusNode<K> {
    private final Logger logger = LoggerFactory.getLogger(LeafNode.class);

    protected LeafNode<K> next;
    protected LeafNode<K> prev;
    protected InternalNode<K> parent;

    private PageManager.Page page; // 直接使用 Page

    public LeafNode(TypeHandler<K> keyHandler, int spaceId, int pageNo) throws IOException {
        super();
        this.keyHandler = keyHandler;
        this.isLeaf = true;
        this.gid = new GlobalPageId(spaceId, pageNo);

        // 初始化或加载页
        page = pageManager.readPageFromDisk(spaceId, pageNo);
        if (page == null) {
            // 第一次创建
            page = new Page(pageNo);
            page.header.pageType = 1; // 索引页
            save(); // 写入磁盘
        }

        this.header = page.header;
        this.keys = new ArrayList<>();
        this.records = new ArrayList<>();
        deserializeKeys(page.getPageData());
    }

    @Override
    protected void deserializeKeys(byte[] pageData) {
        keys.clear();
        records.clear();

        int used = PageManager.PAGE_SIZE - header.freeSpace;
        int offset = PageManager.PAGE_HEADER_SIZE;

        while (offset < used) {
            short keyLen = ByteBuffer.wrap(pageData, offset, 2).getShort();
            offset += 2;

            byte[] keyBytes = Arrays.copyOfRange(pageData, offset, offset + keyLen);
            K key = keyHandler.deserialize(ByteBuffer.wrap(keyBytes));
            offset += keyLen;

            short valLen = ByteBuffer.wrap(pageData, offset, 2).getShort();
            offset += 2;

            byte[] valBytes = Arrays.copyOfRange(pageData, offset, offset + valLen);
            offset += valLen;

            keys.add(key);
            records.add(valBytes);
        }
    }

    @Override
    protected byte[] serializeToPage() {
        ByteBuffer buf = ByteBuffer.wrap(page.getPageData());
        buf.order(ByteOrder.LITTLE_ENDIAN);

        buf.position(PageManager.PAGE_HEADER_SIZE);
        for (int i = 0; i < keys.size(); i++) {
            byte[] keyBytes = keyHandler.serialize(keys.get(i));
            byte[] valBytes = records.get(i);

            buf.putShort((short) keyBytes.length);
            buf.put(keyBytes);
            buf.putShort((short) valBytes.length);
            buf.put(valBytes);
        }

        int used = buf.position();
        header.freeSpace = (short) (PageManager.PAGE_SIZE - used);
        return page.getPageData();
    }

    public GlobalPageId insert(K key, byte[] record, int order) {
        int pos = Collections.binarySearch(keys, key);
        if (pos < 0) pos = -pos - 1;
        if (pos < keys.size() && keys.get(pos).compareTo(key) == 0) {
            records.set(pos, record); // 覆盖
        } else {
            keys.add(pos, key);
            records.add(pos, record);
        }

        if (isOverflow(order)) {
            split(order);
        } else {
            save();
        }
        return gid;
    }

    public boolean isOverflow(int order) {
        return keys.size() > order;
    }

    public void split(int order) {
        int mid = keys.size() / 2;

        int newPageNo;
        try {
            newPageNo = pageManager.allocatePage(gid.spaceId);
        } catch (IOException e) {
            throw new RuntimeException("分裂失败：无法分配新页", e);
        }

        LeafNode<K> newLeaf;
        try {
            newLeaf = new LeafNode<>(keyHandler, gid.spaceId, newPageNo);
        } catch (IOException e) {
            throw new RuntimeException("分裂失败：新页加载异常", e);
        }

        newLeaf.keys.addAll(keys.subList(mid, keys.size()));
        newLeaf.records.addAll(records.subList(mid, records.size()));

        keys.subList(mid, keys.size()).clear();
        records.subList(mid, records.size()).clear();

        newLeaf.next = this.next;
        newLeaf.prev = this;
        this.next = newLeaf;
        if (newLeaf.next != null) newLeaf.next.prev = newLeaf;

        newLeaf.save();
        this.save();

        K splitKey = newLeaf.keys.get(0);
        if (parent == null) {
            InternalNode<K> newRoot = new InternalNode<>(keyHandler);
            newRoot.keys.add(splitKey);
            newRoot.children.add(this.gid);
            newRoot.children.add(newLeaf.gid);
            parent = newRoot;
            try {
                newRoot.save();
            } catch (IOException e) {
                throw new RuntimeException("保存新根失败", e);
            }
        } else {
            try {
                parent.insertKey(splitKey, newLeaf.gid, order);
            } catch (IOException e) {
                throw new RuntimeException("插入父节点失败", e);
            }
        }
    }

    @Override
    public void save() {
        try {
            serializeToPage();
            pageManager.writePageToDisk(gid.spaceId, gid.pageNo, page);
            header.isDirty = false;
        } catch (IOException e) {
            throw new RuntimeException("保存叶子节点失败", e);
        }
    }
}
