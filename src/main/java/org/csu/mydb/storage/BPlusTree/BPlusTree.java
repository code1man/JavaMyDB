package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class BPlusTree {
    private BPlusNode<Key> root;
    private final int order;
    private final PageManager pageManager;
    private final List<Column> tableColumns; // 主键列

    public BPlusTree(int order, int spaceId, PageManager pageManager, List<Column> tableColumns) throws IOException {
        this.order = order;
        this.pageManager = pageManager;
        this.tableColumns = tableColumns;
        // load root
        root = pageManager.loadNode(new PageManager.GlobalPageId(spaceId, 3), null, tableColumns);
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
        // 生成 Key
        Key key = new Key(
                tableColumns.stream().map(col -> rowValues.get(tableColumns.indexOf(col))).collect(Collectors.toList()),
                tableColumns
        );
        insert(root, key, rowValues);
    }

    private void insert(BPlusNode<Key> node, Key key, List<Object> rowValues) throws IOException {
        if (node.isLeaf) {
            LeafNode leaf = (LeafNode) node;
            leaf.insert(key, rowValues, tableColumns, order); // 叶子节点内部已处理分裂

            // 持久化叶子节点
            pageManager.writeLeafNode(leaf, tableColumns);
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;

            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            insert(child, key, rowValues);

            // 持久化内部节点
            pageManager.writeInternalNode(in);
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

            if (removed) pageManager.writeLeafNode(leaf, tableColumns);
            return removed;
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            boolean removed = delete(child, key);

            if (removed) pageManager.writeInternalNode(in);
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

            if (updated) pageManager.writeLeafNode(leaf, tableColumns);
            return updated;
        } else {
            InternalNode in = (InternalNode) node;
            int pos = 0;
            while (pos < in.keys.size() && key.compareTo(in.keys.get(pos)) >= 0) pos++;
            BPlusNode<Key> child = in.getChildAt(pos, tableColumns);
            boolean updated = update(child, key, newRow);

            if (updated) pageManager.writeInternalNode(in);
            return updated;
        }
    }
}
