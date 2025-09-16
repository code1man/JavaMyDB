package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InternalNode extends BPlusNode<Key> {
    public List<Integer> children = new ArrayList<>();

    public InternalNode(PageManager.GlobalPageId gid,
                        PageManager.PageHeader header,
                        PageManager pageManager) {
        super(gid, header, false, pageManager);
    }

    public void insertKey(Key key, BPlusNode<Key> child, int order){
        int pos = findInsertPosition(key);
        keys.add(pos, key);
        children.add(pos + 1, child.gid.pageNo);
        child.parent = this;

        if (keys.size() > order) {
            try {
                split(order);
            } catch (IOException e) {
                logger.error("分离页表失败");
            }
        }
    }

    private int findInsertPosition(Key key) {
        int i = 0;
        while (i < keys.size() && keys.get(i).compareTo(key) < 0) {
            i++;
        }
        return i;
    }

    private void split(int order) throws IOException {
        int mid = keys.size() / 2;
        Key midKey = keys.get(mid);

        InternalNode newNode = new InternalNode(
                new PageManager.GlobalPageId(gid.spaceId, pageManager.allocatePage(gid.spaceId)),
                new PageManager.PageHeader(),
                pageManager
        );

        newNode.keys.addAll(keys.subList(mid + 1, keys.size()));
        newNode.children.addAll(children.subList(mid + 1, children.size()));

        keys = new ArrayList<>(keys.subList(0, mid));
        children = new ArrayList<>(children.subList(0, mid + 1));

        if (parent == null) {
            InternalNode newRoot = new InternalNode(
                    new PageManager.GlobalPageId(gid.spaceId, 3), // root 固定 page3
                    new PageManager.PageHeader(),
                    pageManager
            );
            newRoot.header.pageNo = 3;
            newRoot.keys.add(midKey);
            newRoot.children.add(this.gid.pageNo);
            newRoot.children.add(newNode.gid.pageNo);
            this.parent = newRoot;
            newNode.parent = newRoot;
        } else {
            parent.insertKey(midKey, newNode, order);
        }
    }

    public int getChild(Key key) {
        int pos = findInsertPosition(key);
        return children.get(pos);
    }

    /**
     * 获取指定索引的子节点
     * 如果子节点是 pageNo（磁盘），则动态加载
     *
     * @param idx 子节点索引
     * @param keyColumns 主键列信息，用于构建 Key
     * @return BPlusNode 对象
     */
    public BPlusNode<Key> getChildAt(int idx, List<Column> keyColumns) throws IOException {
        if (idx < 0 || idx >= keys.size()) {
            return null;
        }
        int childPageNo = children.get(idx);

        // 从磁盘加载节点
        BPlusNode<Key> childNode = pageManager.loadNode(new PageManager.GlobalPageId(gid.spaceId, childPageNo), this, keyColumns);
        children.set(idx, childNode.gid.pageNo); // 缓存到内存
        return childNode;
    }
}
