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
import java.util.List;

import static org.csu.mydb.storage.PageManager.*;

public class InternalNode<K extends Comparable<K>> extends BPlusNode<K> {
    private final Logger logger = LoggerFactory.getLogger(InternalNode.class);

    protected List<GlobalPageId> children;
    private PageManager.Page page;

    public InternalNode(TypeHandler<K> keyHandler, int spaceId, int pageNo) throws IOException {
        super();
        this.keyHandler = keyHandler;
        this.isLeaf = false;
        this.gid = new GlobalPageId(spaceId, pageNo);

        page = pageManager.readPageFromDisk(spaceId, pageNo);
        if (page == null) {
            page = new Page(pageNo);
            page.header = new PageHeader();
            page.header.pageType = 2; // InternalNode 页
            save();
        }

        this.header = page.header;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        deserializeKeys(page.getPageData());
    }

    @Override
    protected void deserializeKeys(byte[] pageData) {
        keys.clear();
        children.clear();

        int used = PageManager.PAGE_SIZE - header.freeSpace;
        int offset = PageManager.PAGE_HEADER_SIZE;

        // InternalNode 格式：
        // [childPageId][key][childPageId][key]...[childPageId]
        // 注意第一个 childPageId 是左指针

        if (used <= PageManager.PAGE_HEADER_SIZE) return;

        // 第一个 childPageId
        int child = ByteBuffer.wrap(pageData, offset, 4).getInt();
        children.add(new GlobalPageId(gid.spaceId, child));
        offset += 4;

        while (offset < used) {
            short keyLen = ByteBuffer.wrap(pageData, offset, 2).getShort();
            offset += 2;

            byte[] keyBytes = Arrays.copyOfRange(pageData, offset, offset + keyLen);
            K key = keyHandler.deserialize(ByteBuffer.wrap(keyBytes));
            offset += keyLen;

            int childId = ByteBuffer.wrap(pageData, offset, 4).getInt();
            offset += 4;

            keys.add(key);
            children.add(new GlobalPageId(gid.spaceId, childId));
        }
    }

    @Override
    protected byte[] serializeToPage() {
        ByteBuffer buf = ByteBuffer.wrap(page.getPageData());
        buf.order(ByteOrder.LITTLE_ENDIAN);

        buf.position(PageManager.PAGE_HEADER_SIZE);

        // [child][key][child][key]...[child]
        buf.putInt(children.get(0).pageNo);
        for (int i = 0; i < keys.size(); i++) {
            byte[] keyBytes = keyHandler.serialize(keys.get(i));
            buf.putShort((short) keyBytes.length);
            buf.put(keyBytes);
            buf.putInt(children.get(i + 1).pageNo);
        }

        int used = buf.position();
        header.freeSpace = (short) (PageManager.PAGE_SIZE - used);
        return page.getPageData();
    }

    public void insertKey(K key, GlobalPageId newChild, int order) throws IOException {
        int pos = Collections.binarySearch(keys, key);
        if (pos < 0) pos = -pos - 1;

        keys.add(pos, key);
        children.add(pos + 1, newChild);

        if (isOverflow(order)) {
            split(order);
        } else {
            save();
        }
    }

    public boolean isOverflow(int order) {
        return keys.size() > order;
    }

    public void split(int order) throws IOException {
        int mid = keys.size() / 2;

        K midKey = keys.get(mid);

        int newPageNo = pageManager.allocatePage(gid.spaceId);
        InternalNode<K> newNode = new InternalNode<>(keyHandler, gid.spaceId, newPageNo);

        newNode.keys.addAll(keys.subList(mid + 1, keys.size()));
        newNode.children.addAll(children.subList(mid + 1, children.size()));

        keys.subList(mid, keys.size()).clear();
        children.subList(mid + 1, children.size()).clear();

        newNode.save();
        this.save();

        if (parent == null) {
            InternalNode<K> newRoot = new InternalNode<>(keyHandler, gid.spaceId, pageManager.allocatePage(gid.spaceId));
            newRoot.keys.add(midKey);
            newRoot.children.add(this.gid);
            newRoot.children.add(newNode.gid);
            parent = newRoot;
            newRoot.save();
        } else {
            parent.insertKey(midKey, newNode.gid, order);
        }
    }

    @Override
    public void save() {
        try {
            serializeToPage();
            pageManager.writePageToDisk(gid.spaceId, gid.pageNo, page);
            header.isDirty = false;
        } catch (IOException e) {
            throw new RuntimeException("保存内部节点失败", e);
        }
    }
}

