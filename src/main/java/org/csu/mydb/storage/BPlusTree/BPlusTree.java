package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.storage.storageFiles.page.PageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BPlusTree {
    private BPlusNode<Key> root;
    private final int order;
    private final StorageSystem storageSystem;
    private final List<Column> tableColumns; // 主键列
    public final static int ROOT_PAGE_NO = 3;

    public BPlusTree(int order, int spaceId, StorageSystem storageSystem, List<Column> tableColumns) throws IOException {
        this.order = order;
        this.storageSystem = storageSystem;
        this.tableColumns = tableColumns;
        // load root
        root = storageSystem.loadNode(new PageManager.GlobalPageId(spaceId, 3), null, tableColumns);
    }

    public List<Column> getColumns() {
        return tableColumns;
    }

    // ======================== 查找 ========================
    /**
     * 根据主键查找
     * @param key 主键
     * @return 行
     */
    public List<Object> search(Key key) throws IOException {
        return search(root, key);
    }

    private List<Object> search(BPlusNode<Key> node, Key key) throws IOException {
        if (node.isLeaf) {
            return ((LeafNode) node).search(key);
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;

            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            return search(child, key);
        }
    }

    // ======================== 插入 ========================
    /**
     * 按照主键插入
     * @param rowValues 行
     */
    public void insert(List<Object> rowValues) throws IOException {
        // 根据主键列生成 Key，并加入 keys
        List<Object> keyValues = new ArrayList<>();
        List<Column> pkColumns = new ArrayList<>();
        for (int j = 0; j < tableColumns.size(); j++) {
            Column col = tableColumns.get(j);
            if (col.isPrimaryKey()) {
                keyValues.add(rowValues.get(j));
                pkColumns.add(col);
            }
        }
        Key key = new Key(keyValues, pkColumns);

        BPlusNode<Key> maybeNewRoot = insertRecursive(root, key, rowValues);
        if (maybeNewRoot != null) {
            this.root = maybeNewRoot;
        }
    }

    private BPlusNode<Key> insertRecursive(BPlusNode<Key> node, Key key, List<Object> rowValues) throws IOException {
        if (node.isLeaf) {
            LeafNode leaf = (LeafNode) node;

            // 插入到叶子，可能返回新的 root
            BPlusNode<Key> maybeNewRoot = leaf.insert(key, rowValues, tableColumns, order);
            if (maybeNewRoot != null && maybeNewRoot.header.pageNo == ROOT_PAGE_NO)
                return maybeNewRoot;
            return null;
        } else {
            InternalNode in = (InternalNode) node;

            // 1. 找到子节点索引
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;

            BPlusNode<Key> child;
            // 2. 确保子节点存在
            if (pos < in.children.size()) {
                // 已有子节点，从磁盘加载
                int childPageNo = in.children.get(pos);
                child = storageSystem.loadNode(new PageManager.GlobalPageId(in.gid.spaceId, childPageNo), in, tableColumns);
            } else {
                // 子节点不存在 -> 新建叶子节点
                int newChildPageNo = storageSystem.getPageManager().allocatePage(in.gid.spaceId);
                PageManager.PageHeader header = new PageManager.PageHeader();
                header.pageType = PageType.DATA_PAGE;
                header.pageNo = newChildPageNo;
                header.prevPage = in.children.isEmpty() ? -1 : in.children.get(in.children.size() - 1);

                LeafNode newLeaf = new LeafNode(new PageManager.GlobalPageId(in.gid.spaceId, newChildPageNo), header, storageSystem);
                in.children.add(newChildPageNo);
                child = newLeaf;

                // 写入缓存
                storageSystem.writeLeafNode(newLeaf, tableColumns);
            }

            // 3. 递归插入
            BPlusNode<Key> maybeNewRoot = insertRecursive(child, key, rowValues);

            // 4. 写回当前 internal node
            storageSystem.writeInternalNode(in, tableColumns);

            // 5. 检查是否需要分裂 internal node
            if (in.keys.size() > order) {
                InternalNode splitRoot = in.split(order, tableColumns);
                if (splitRoot.gid.pageNo == BPlusTree.ROOT_PAGE_NO) {
                    return splitRoot;
                }
            }

            if (maybeNewRoot != null) {
                this.root = maybeNewRoot;
            }

            return maybeNewRoot; // 下层分裂生成的 root 或 null
        }
    }




    // ======================== 删除 ========================
    public boolean delete(Key key) throws IOException {
        return delete(root, key);
    }

    private boolean delete(BPlusNode<Key> node, Key key) throws IOException {
        if (node.isLeaf) {
            LeafNode leaf = (LeafNode) node;
            boolean removed = leaf.delete(key, tableColumns);

            if (removed) storageSystem.writeLeafNode(leaf, tableColumns);
            return removed;
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            boolean removed = delete(child, key);

            if (removed) storageSystem.writeInternalNode(in, tableColumns);
            return removed;
        }
    }

    // ======================== 更新 ========================
    public boolean update(Key key, List<Object> newRow) throws IOException {
        return update(root, key, newRow);
    }

    private boolean update(BPlusNode<Key> node, Key key, List<Object> newRow) throws IOException {
        if (node.isLeaf) {
            LeafNode leaf = (LeafNode) node;
            boolean updated = leaf.update(key, newRow, tableColumns);

            if (updated) storageSystem.writeLeafNode(leaf, tableColumns);
            return updated;
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            boolean updated = update(child, key, newRow);

            if (updated) storageSystem.writeInternalNode(in, tableColumns);
            return updated;
        }
    }
}
