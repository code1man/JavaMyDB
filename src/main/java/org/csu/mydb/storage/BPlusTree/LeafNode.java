package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Column.RecordSerializer;
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

    public LeafNode(PageManager.GlobalPageId gid,
                    PageManager.PageHeader header,
                    StorageSystem storageSystem) {
        super(gid, header, true, storageSystem);
    }

    /**
     * 插入：返回值如果造成 root 变化则返回新的 root（InternalNode），否则返回 null。
     *
     * @param key          主键
     * @param rowData      行数据（按表列顺序）
     * @param tableColumns 表列定义（用于持久化）
     * @param order        B+树阶（叶子允许的最大键数为 order）
     */
    public BPlusNode<Key> insert(Key key, List<Object> rowData, List<Column> tableColumns, int order) {
        int pos = findInsertPosition(key);
        final int space = gid.spaceId;
        final String filePath = "G:\\MyDB\\MyDB\\src\\main\\resources\\test\\jb.idb";

        // 1) 如果主键已存在，覆盖
        if (pos < keys.size() && keys.get(pos).compareTo(key) == 0) {
            records.set(pos, rowData);

            try {
                // 覆盖写缓存页
                byte[] data = RecordSerializer.serializeDataRow(rowData, tableColumns);
                StorageSystem.writePage(filePath, space, this.gid.pageNo, data, tableColumns);
            } catch (Exception e) {
                logger.error("Failed to overwrite leaf record in cache", e);
            }
            return null;
        }

        // 2) 插入到内存结构
        keys.add(pos, key);
        records.add(pos, rowData);

        // 3) 写入缓存页（新增）
        try {
            byte[] data = RecordSerializer.serializeDataRow(rowData, tableColumns);
            StorageSystem.writePage(filePath, space, this.gid.pageNo, data, tableColumns);
        } catch (Exception e) {
            logger.error("Failed to insert leaf record in cache", e);
        }

        // 4) 如果超过阶，执行 split
        if (keys.size() > order) {
            try {
                return split(order, tableColumns);
            } catch (IOException e) {
                logger.error("Failed to split leaf node", e);
                return null;
            }
        }

        return null;
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

    /**
     * 分裂叶子节点
     * - 普通分裂：parent != null -> 返回右节点给 parent 插入
     * - root-special-case：parent == null && gid.pageNo == ROOT_PAGE_NO -> 新建 root
     *
     * @param order B+树阶
     * @param tableColumns 表列定义
     * @return 如果生成新的 root，返回新的 InternalNode root，否则返回 null
     * @throws IOException
     */
    public BPlusNode<Key> split(int order, List<Column> tableColumns) throws IOException {
        int midIndex = keys.size() / 2;

        // 左右两部分
        List<Key> leftKeys = new ArrayList<>(keys.subList(0, midIndex));
        List<List<Object>> leftRecords = new ArrayList<>(records.subList(0, midIndex));

        List<Key> rightKeys = new ArrayList<>(keys.subList(midIndex, keys.size()));
        List<List<Object>> rightRecords = new ArrayList<>(records.subList(midIndex, records.size()));

        // 更新当前叶子为左半部分
        this.keys = leftKeys;
        this.records = leftRecords;

        // 创建右叶子节点
        int newLeafPageNo = storageSystem.getPageManager().allocatePage(gid.spaceId);
        PageManager.PageHeader rightHeader = new PageManager.PageHeader();
        rightHeader.pageType = PageType.DATA_PAGE;
        rightHeader.pageNo = newLeafPageNo;
        rightHeader.prevPage = this.gid.pageNo;
        rightHeader.nextPage = this.header.nextPage; // 原 next 指向继承

        LeafNode rightLeaf = new LeafNode(new PageManager.GlobalPageId(gid.spaceId, newLeafPageNo),
                rightHeader, storageSystem);
        rightLeaf.keys = rightKeys;
        rightLeaf.records = rightRecords;

        // 更新链表指针
        this.header.nextPage = newLeafPageNo;

        // 写回缓存
        storageSystem.writeLeafNode(this, tableColumns);
        storageSystem.writeLeafNode(rightLeaf, tableColumns);

        // root-special-case
        if (gid.pageNo == BPlusTree.ROOT_PAGE_NO) {
            // 原叶子节点 Page3 升级为 InternalNode
            InternalNode newRoot = new InternalNode(
                    new PageManager.GlobalPageId(gid.spaceId, BPlusTree.ROOT_PAGE_NO),
                    new PageManager.PageHeader() {{
                        pageType = PageType.INDEX_PAGE;
                        pageNo = BPlusTree.ROOT_PAGE_NO;
                    }},
                    storageSystem
            );

            // 左右叶子分别写入新的页
            int rightLeafPageNo = storageSystem.getPageManager().allocatePage(gid.spaceId);
            rightHeader = new PageManager.PageHeader();
            rightHeader.pageType = PageType.DATA_PAGE;
            rightHeader.pageNo = rightLeafPageNo;
            rightHeader.prevPage = this.gid.pageNo;
            rightHeader.nextPage = this.header.nextPage;

            rightLeaf = new LeafNode(new PageManager.GlobalPageId(gid.spaceId, rightLeafPageNo),
                    rightHeader, storageSystem);
            rightLeaf.keys = rightKeys;
            rightLeaf.records = rightRecords;

            this.keys = leftKeys;
            this.records = leftRecords;
            this.header.nextPage = rightLeafPageNo;

            // 新 root 指向左右叶子
            newRoot.keys.add(rightKeys.get(0));
            newRoot.children.add(this.gid.pageNo);
            newRoot.children.add(rightLeafPageNo);

            // 写回
            storageSystem.writeLeafNode(this, tableColumns);
            storageSystem.writeLeafNode(rightLeaf, tableColumns);
            storageSystem.writeInternalNode(newRoot, tableColumns);

            return newRoot; // BPlusTree.root 指向 Page3
        }



        // 普通分裂返回右叶子，让 parent 插入
        return rightLeaf;
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
}
