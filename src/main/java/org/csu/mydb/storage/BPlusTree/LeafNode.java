package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.storage.storageFiles.page.PageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 叶子节点实现（聚簇索引样式）
 * - records: 每条记录是 List<Object>（整行列值）
 * - keys: 在父类 BPlusNode 中声明（List<Key>）
 * insert(...) 返回值：
 *   - 如果分裂导致 root 更改（root-special-case），返回新的 root（InternalNode）
 *   - 否则返回 null
 */
public class LeafNode extends BPlusNode<Key> {
    // 每条记录是表列的值列表
    public List<List<Object>> records = new ArrayList<>();
    public LeafNode next;
    public LeafNode pre;

    public LeafNode(PageManager.GlobalPageId gid,
                    PageManager.PageHeader header,
                    StorageSystem storageSystem) {
        super(gid, header, true, storageSystem);
    }

    /**
     * 查找插入位置（保持 key 列表有序）
     */
    private int findInsertPosition(Key key) {
        int i = 0;
        if (keys.isEmpty())
            return i;
        while (i < keys.size() && keys.get(i).compareTo(key) < 0) {
            i++;
        }
        return i;
    }

    public BPlusNode<Key> insert(Key key, List<Object> rowData, List<Column> tableColumns, int order) throws IOException {
        int pos = 0;
        while (pos < keys.size() && key.compareTo(keys.get(pos)) > 0) pos++;

        // 覆盖已存在 key
        if (pos < keys.size() && keys.get(pos).compareTo(key) == 0) {
            records.set(pos, rowData);
            storageSystem.writeLeafNode(this, tableColumns);
            return null;
        }

        keys.add(pos, key);
        records.add(pos, rowData);
        storageSystem.writeLeafNode(this, tableColumns);

        if (keys.size() > order) return split(order, tableColumns);
        return null;
    }

    /**
     * 叶子节点分裂
     */
    public InternalNode split(int order, List<Column> tableColumns) throws IOException {
        // 1. 分裂位置
        int mid = keys.size() / 2;

        // 2. 新的右兄弟
        int newPageNo = storageSystem.getPageManager().allocatePage(gid.spaceId);
        LeafNode right = new LeafNode(
                new PageManager.GlobalPageId(gid.spaceId, newPageNo),
                new PageManager.PageHeader(newPageNo, PageType.DATA_PAGE),
                storageSystem
        );

        // 3. 拷贝右半部分
        right.keys.addAll(keys.subList(mid, keys.size()));
        right.records.addAll(records.subList(mid, records.size()));

        // 4. 保留左半部分
        this.keys = new ArrayList<>(keys.subList(0, mid));
        this.records = new ArrayList<>(records.subList(0, mid));

        // 5. 双向链表维护
        right.header.nextPage = this.header.nextPage;
        if (this.header.nextPage != -1) {
            PageManager.Page nextPage = storageSystem.getPageManager().getPage(gid.spaceId, this.header.nextPage);
            nextPage.getHeader().prevPage = right.gid.pageNo;
            storageSystem.getBufferPool().putPage(nextPage, gid.spaceId);
        }
        right.header.prevPage = this.gid.pageNo;
        this.header.nextPage = right.gid.pageNo;

        // 6. 持久化左右节点
        storageSystem.writeLeafNode(this, tableColumns);
        storageSystem.writeLeafNode(right, tableColumns);

        // 7. 返回要上提的 key（右兄弟的第一个 key）
        Key upKey = right.keys.get(0);

        InternalNode parent = new InternalNode(
                new PageManager.GlobalPageId(gid.spaceId, storageSystem.getPageManager().allocatePage(gid.spaceId)),
                new PageManager.PageHeader(-1, PageType.INDEX_PAGE),
                storageSystem
        );
        parent.header.pageNo = parent.gid.pageNo;
        parent.keys.add(upKey);
        parent.children.add(this.gid.pageNo);
        parent.children.add(right.gid.pageNo);

        storageSystem.writeInternalNode(parent, tableColumns);

        return parent;
    }

    public SplitResult<Key> insertAndMaybeSplit(Key key, List<Object> rowValues, List<Column> tableColumns, int order) throws IOException {
        int pos = 0;
        while (pos < keys.size() && key.compareTo(keys.get(pos)) > 0) pos++;

        if (pos < keys.size() && key.compareTo(keys.get(pos)) == 0) {
            records.set(pos, rowValues);
            storageSystem.writeLeafNode(this, tableColumns);
            return null;
        }

        keys.add(pos, key);
        records.add(pos, rowValues);
        storageSystem.writeLeafNode(this, tableColumns);

        if (keys.size() <= order) return null;

        // 分裂
        int mid = keys.size() / 2;
        LeafNode right = new LeafNode(new PageManager.GlobalPageId(gid.spaceId, storageSystem.getPageManager().allocatePage(gid.spaceId)),new PageManager.PageHeader(),storageSystem);
        right.keys.addAll(keys.subList(mid, keys.size()));
        right.records.addAll(records.subList(mid, records.size()));
        keys = new ArrayList<>(keys.subList(0, mid));
        records = new ArrayList<>(records.subList(0, mid));

        right.next = this.next;
        this.next = right;

        storageSystem.writeLeafNode(this, tableColumns);
        storageSystem.writeLeafNode(right, tableColumns);

        return new SplitResult<Key>(right.keys.get(0), right.gid.pageNo);
    }



    /**
     * 寻找节点
     * @param key 主键
     * @return 返回行
     */
    public List<Object> search(Key key) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) == 0) {
                return records.get(i);
            }
        }
        return null;
    }

    /**
     * 更新某行（通过主键）
     *
     * @param key          主键
     * @param newRow       新行数据
     * @param tableColumns 表列定义（用于持久化）
     * @return 更新是否成功
     */
    public boolean update(Key key, List<Object> newRow, List<Column> tableColumns) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) == 0) {
                records.set(i, newRow);
                storageSystem.writeLeafNode(this, tableColumns);
                return true;
            }
        }
        return false;
    }

    /**
     * 删除某行（通过主键）
     *
     * @param key          主键
     * @param tableColumns 表列定义（用于持久化）
     * @return 是否删除成功
     */
    public boolean delete(Key key, List<Column> tableColumns) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) == 0) {
                keys.remove(i);
                records.remove(i);
                storageSystem.writeLeafNode(this, tableColumns);
                return true;
            }
        }
        return false;
    }

    private Key checkKey(Key key) {
        if (key.getValues().size() != key.getKeyColumns().size()) {
            throw new IllegalStateException("Malformed Key: values.size != keyColumns.size | page=" + this.gid.pageNo);
        }
        return key;
    }
}
