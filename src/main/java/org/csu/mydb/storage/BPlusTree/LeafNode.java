package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LeafNode extends BPlusNode<Key> {
    // 每条记录是表列的值列表
    public final List<List<Object>> records = new ArrayList<>();

    public LeafNode(PageManager.GlobalPageId gid,
                    PageManager.PageHeader header,
                    PageManager pageManager) {
        super(gid, header, true, pageManager);
    }

    /**
     * 插入
     * @param key 主键
     * @param rowData 一行数据
     * @param order 层数
     */
    public void insert(Key key, List<Object> rowData, List<Column> tableColumns, int order) {
        int pos = findInsertPosition(key);
        keys.add(pos, key);
        records.add(pos, rowData);

        // 写回磁盘
        try {
            pageManager.writeLeafNode(this, tableColumns);
        } catch (IOException e) {
            logger.error("写回磁盘失败");
        }

        if (keys.size() > order) {
            try {
                split(order, tableColumns);
            } catch (IOException e) {
                logger.error("Failed to split record", e);
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

    /**
     * 分裂
     * @param order 层数
     */
    private void split(int order, List<Column> tableColumns) throws IOException {
        int mid = keys.size() / 2;

        LeafNode newNode = new LeafNode(
                new PageManager.GlobalPageId(gid.spaceId, pageManager.allocatePage(gid.spaceId)),
                new PageManager.PageHeader(),
                pageManager
        );

        newNode.keys.addAll(keys.subList(mid, keys.size()));
        newNode.records.addAll(records.subList(mid, records.size()));

        keys = new ArrayList<>(keys.subList(0, mid));
        records.retainAll(records.subList(0, mid));

        Key midKey = newNode.keys.get(0);

        // 写磁盘
        pageManager.writeLeafNode(this, tableColumns);
        pageManager.writeLeafNode(newNode, tableColumns);

        if (parent == null) {
            InternalNode newRoot = new InternalNode(
                    new PageManager.GlobalPageId(gid.spaceId, 3), // root 固定 page3
                    new PageManager.PageHeader(),
                    pageManager
            );
            newRoot.header.pageNo = 3;
            newRoot.keys.add(midKey);
            newRoot.children.add(gid.pageNo);
            newRoot.children.add(newNode.gid.pageNo);
            this.parent = newRoot;
            newNode.parent = newRoot;

            pageManager.writeInternalNode(newRoot);
        } else {
            parent.insertKey(midKey, newNode, order);
        }
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
     * 更新节点信息
     *
     * @param key    主键
     * @param newRow 新的一行数据
     * @return 是否更新成功
     */
    public boolean update(Key key, List<Object> newRow, List<Column> tableColumns) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) == 0) {
                records.set(i, newRow);
                try {
                    pageManager.writeLeafNode(this, tableColumns);
                } catch (IOException e) {
                    logger.error("无法持久化");
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    /**
     * 删除节点
     *
     * @param key 主键
     * @return 是否删除成功
     */
    public boolean delete(Key key, List<Column> tableColumns) {
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) == 0) {
                keys.remove(i);
                records.remove(i);
                try {
                    pageManager.writeLeafNode(this, tableColumns);
                } catch (IOException e) {
                    logger.error("无法在磁盘执行该操作");
                    return false;
                }
                return true;
            }
        }
        return false;
    }
}
