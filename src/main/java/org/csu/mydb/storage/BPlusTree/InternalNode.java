package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Column.RecordSerializer;
import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.storage.storageFiles.page.PageType;
import org.csu.mydb.util.Pair.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 内部节点（children 存储为 pageNo）：当需要访问子节点时，通过 pageManager.loadNode(...) 动态加载
 */
public class InternalNode extends BPlusNode<Key> {
    // children 存储为 pageNo（磁盘页号）
    public List<Integer> children = new ArrayList<>();

    public InternalNode(PageManager.GlobalPageId gid,
                        PageManager.PageHeader header,
                        StorageSystem storageSystem) {
        super(gid, header, false, storageSystem);
    }

    private int findInsertPosition(Key key) {
        int i = 0;
        while (i < keys.size() && keys.get(i).compareTo(key) < 0) {
            i++;
        }
        return i;
    }

    /**
     * 插入 key + child (child 为 BPlusNode)
     * 返回值：
     *   - 如果向上冒泡并产生新的 root（root-special-case）会返回 new root (InternalNode)
     *   - 否则返回 null
     */
    public BPlusNode<Key> insertKey(Key key, BPlusNode<Key> child, int order, List<Column> tableColumns) throws IOException {
        // 找到 key 插入位置
        int pos = findInsertPosition(key);
        keys.add(pos, key);
        children.add(pos + 1, child.gid.pageNo);
        child.parent = this;

        // 持久化当前 internal node
        storageSystem.writeInternalNode(this, tableColumns);

        // 超过阶数 -> split
        if (keys.size() > order) {
            return split(order, tableColumns);
        }

        return null;
    }

    /**
     * 内部节点分裂
     */
    public InternalNode split(int order, List<Column> tableColumns) throws IOException {
        int mid = keys.size() / 2;
        Key promoteKey = keys.get(mid);

        int space = gid.spaceId;
        int newPageNo = storageSystem.getPageManager().allocatePage(space);

        PageManager.PageHeader header = new PageManager.PageHeader();
        header.pageType = PageType.INDEX_PAGE;
        header.pageNo = newPageNo;

        InternalNode rightNode = new InternalNode(
                new PageManager.GlobalPageId(space, newPageNo),
                header,
                storageSystem
        );

        // 左边保留 [0, mid)
        // 右边保留 [mid+1, end)
        rightNode.keys.addAll(keys.subList(mid + 1, keys.size()));
        rightNode.children.addAll(children.subList(mid + 1, children.size()));

        // 截断左边
        keys = new ArrayList<>(keys.subList(0, mid));
        children = new ArrayList<>(children.subList(0, mid + 1));

        // 写磁盘
        storageSystem.writeInternalNode(this, tableColumns);
        storageSystem.writeInternalNode(rightNode, tableColumns);

        return rightNode;  // 保证 rightNode 有 key
    }

    /**
     * 根据主键找到子节点页号（用于直接查找的场景）
     * @param key 主键
     * @return 子节点 pageNo
     */
    public int getChild(Key key) {
        int pos = findInsertPosition(key);
        return children.get(pos);
    }

    /**
     * 获取指定索引的子节点（动态从磁盘加载）
     * 如果 children 列表长度不足，会尝试从磁盘页重新解析并重建 children 列表。
     * @param idx 子节点索引（0..children.size()-1）
     * @param keyColumns 用于构建 Key 的表列（传递给 pageManager.loadNode）
     * @return 装载出的 BPlusNode（叶或内部）
     * @throws IOException 读磁盘或反序列化错误时抛出
     */
    public BPlusNode<Key> getChildAt(int idx, List<Column> keyColumns) throws IOException {
        if (idx < 0) throw new IndexOutOfBoundsException("Child index must be >= 0, but got " + idx);

        // 如果 children 数量不足以包含 idx，则尝试从磁盘 page 解析并重建 children 指针
        if (idx >= children.size()) {
            // 尝试从磁盘读取当前 InternalNode 的页面并解析 child 指针到 children 列表
            PageManager.Page page = pageManager.readPage(gid.spaceId, gid.pageNo);

            if (page == null) {
                throw new IOException("Cannot read internal node page from disk: " + gid.pageNo);
            }

            rebuildChildrenFromPage(page, keyColumns);
        }

        if (idx >= children.size()) {
            // 解析后仍然不足，说明页内数据不一致（序列化或元数据有问题）
            throw new IOException(String.format("Internal node children pointer missing after rebuild: page=%d keys=%d children=%d requestedIdx=%d",
                    gid.pageNo, keys.size(), children.size(), idx));
        }

        int childPageNo = children.get(idx);
        // 动态从磁盘加载子节点（loadNode 会根据 pageType 返回 LeafNode 或 InternalNode）
        BPlusNode<Key> childNode = storageSystem.loadNode(new PageManager.GlobalPageId(gid.spaceId, childPageNo), this, keyColumns);

        return childNode;
    }

    /**
     * 从 page 的二进制数据重建 children 列表（解析序列化格式）
     * 假设 internal node page 格式类似：
     *   [childPageId:int][keyLen:short][keyBytes][childPageId:int]...[keyLen:short][keyBytes][childPageId:int]
     * 注意：根据你实际的 serializeToPage 实现可能需要调整字节序或字段顺序。
     */
    private void rebuildChildrenFromPage(PageManager.Page page, List<Column> keyColumns) throws IOException {
        this.children.clear();
        this.keys.clear();
        List<byte[]> records = new ArrayList<>();
        for (int i = 0; i < page.header.recordCount; i++) {
            records.add(page.getRecord(i));
        }

        if (records.isEmpty()) {
            throw new IOException("Internal node page has no records: " + gid.pageNo);
        }

        // 后续 record = (key, child)
        for (byte[] record : records) {
            Pair<Key, Integer> pair = RecordSerializer.deserializeKeyPtr(record, keyColumns);
            this.keys.add(pair.getFirst());
            this.children.add(pair.getSecond());
        }
    }

    private Key checkKey(Key key) {
        if (key.getValues().size() != key.getKeyColumns().size()) {
            throw new IllegalStateException("Malformed Key at page=" + gid.pageNo);
        }
        return key;
    }
}
