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
    private final String filePath;

    public BPlusTree(int order, int spaceId, StorageSystem storageSystem, List<Column> tableColumns, String filePath) throws IOException {
        this.order = 100;
        this.storageSystem = storageSystem;
        this.tableColumns = tableColumns;
        this.filePath = filePath;

        // 从 Page2 读取 root 页号
        int rootPageNo = storageSystem.getRootPageNo(spaceId);

        // 如果 Page2 为空（第一次建树），就创建 root leaf
        if (rootPageNo == -1) {
            int newRootPage = storageSystem.getPageManager().allocatePage(spaceId);
            PageManager.PageHeader header = new PageManager.PageHeader();
            header.pageNo = newRootPage;
            header.pageType = PageType.DATA_PAGE;

            LeafNode rootLeaf = new LeafNode(new PageManager.GlobalPageId(spaceId, newRootPage), header, storageSystem);
            storageSystem.writeLeafNode(filePath, rootLeaf, tableColumns);

            // 更新 Page2
            storageSystem.updateRootPageNo(filePath, spaceId, newRootPage);

            this.root = rootLeaf;
        } else {
            // 正常加载 root 节点
            this.root = storageSystem.loadNode(filePath, new PageManager.GlobalPageId(spaceId, rootPageNo), null, tableColumns);
        }
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
    public void insert(List<Column> cols, List<Object> rowValues) throws IOException {
        // 1. 构造 Key
        List<Object> keyValues = new ArrayList<>();
        List<Column> pkColumns = new ArrayList<>();
        for (int j = 0; j < cols.size(); j++) {
            Column col = cols.get(j);
            if (col.isPrimaryKey()) {
                keyValues.add(rowValues.get(j));
                pkColumns.add(col);
            }
        }
        Key key = new Key(keyValues, pkColumns);
        int j = 0;
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < tableColumns.size(); i++) {
            if (tableColumns.get(i).getName().equals(cols.get(j).getName())) {
                values.add(rowValues.get(j));
                j++;
            } else {
                values.add(tableColumns.get(i).getDefaultValue());
            }
        }

        // 2. 递归插入
        BPlusNode<Key> newRoot = insertRecursive(root, key, rowValues);

        // 3. 如果返回了新的 root，更新树和系统 root 页号
        if (newRoot != null) {
            this.root = newRoot;
            storageSystem.updateRootPageNo(filePath, root.gid.spaceId, root.gid.pageNo);
        }
    }



    private BPlusNode<Key> insertRecursive(BPlusNode<Key> node, Key key, List<Object> rowValues) throws IOException {
        if (node.isLeaf) {
            LeafNode leaf = (LeafNode) node;
            BPlusNode<Key> newRoot = leaf.insert(key, rowValues, tableColumns, order);
            return newRoot; // Leaf 插入不会生成 root，除非当前是 root 且分裂
        } else {
            InternalNode in = (InternalNode) node;

            // 找到子节点位置
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = storageSystem.loadNode(filePath,
                    new PageManager.GlobalPageId(in.gid.spaceId, in.children.get(pos)), in, tableColumns);

            // 递归插入
            BPlusNode<Key> maybeNewRoot = insertRecursive(child, key, rowValues);

            // 回溯：内部节点分裂
            if (in.keys.size() > order) {
                InternalNode splitRoot = in.split(order, tableColumns);
                if (splitRoot != null) {
                    // 更新 Page2
                    storageSystem.updateRootPageNo(filePath, splitRoot.gid.spaceId, splitRoot.gid.pageNo);
                    return splitRoot;
                }
            }

            // 持久化 internal node
            storageSystem.writeInternalNode(filePath, in, tableColumns);
            return maybeNewRoot;
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

            if (removed) storageSystem.writeLeafNode(filePath, leaf, tableColumns);
            return removed;
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            boolean removed = delete(child, key);

            if (removed) storageSystem.writeInternalNode(filePath, in, tableColumns);
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

            if (updated) storageSystem.writeLeafNode(filePath, leaf, tableColumns);
            return updated;
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            boolean updated = update(child, key, newRow);

            if (updated) storageSystem.writeInternalNode(filePath, in, tableColumns);
            return updated;
        }
    }
}
