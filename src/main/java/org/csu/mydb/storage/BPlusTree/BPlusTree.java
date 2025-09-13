package org.csu.mydb.storage.BPlusTree;

import org.csu.mydb.storage.DBMS;
import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.bufferPool.BufferPool;

import java.io.IOException;
import java.util.*;

public class BPlusTree<K extends Comparable<K>> {

    private final int order;
    private BPlusNode root;

    public BPlusTree(int order) {
        this.order = Math.max(3, order); // 最小阶数保护
        this.root = new LeafNode();
    }

    // ======= 节点抽象 =======
    abstract class BPlusNode {
        boolean isLeaf;
        List<K> keys;
        BPlusNode parent;

        abstract boolean isOverflow();
        abstract boolean isUnderflow();
    }

    class InternalNode extends BPlusNode {
        List<BPlusNode> children;

        InternalNode() {
            this.isLeaf = false;
            this.keys = new ArrayList<>();
            this.children = new ArrayList<>();
        }

        @Override
        boolean isOverflow() {
            return children.size() > order;
        }

        @Override
        boolean isUnderflow() {
            int min = (order + 1) / 2;
            return children.size() < min;
        }
    }

    class LeafNode extends BPlusNode {
        List<PageManager.GlobalPageId> values;
        LeafNode next;
        LeafNode prev;

        LeafNode() {
            this.isLeaf = true;
            this.keys = new ArrayList<>();
            this.values = new ArrayList<>();
        }

        @Override
        boolean isOverflow() {
            return values.size() > order - 1;
        }

        @Override
        boolean isUnderflow() {
            int min = Math.max(1, order / 2);
            return values.size() < min;
        }
    }

    // ========== 公共接口 ==========

    /**
     * 根据 key 查找对应的 DataPage（若索引不存在则返回 null）
     */
    public PageManager.DataPage search(K key) throws IOException {
        PageManager.GlobalPageId gid = searchGid(key);
        if (gid == null) return null;
        return DBMS.BUFFER_POOL.getPage(gid);
    }

    /**
     * 查找并返回 GlobalPageId（索引层面）
     */
    public PageManager.GlobalPageId searchGid(K key) {
        LeafNode leaf = findLeaf(key);
        int idx = Collections.binarySearch(leaf.keys, key);
        return idx >= 0 ? leaf.values.get(idx) : null;
    }

    /**
     * 范围查询
     * @param start 起始 key（包含）
     * @param end   结束 key（包含）
     * @return 所有匹配记录的数据页（通过 BufferPool 获取）
     */
    public List<PageManager.DataPage> rangeSearch(K start, K end) throws IOException {
        List<PageManager.DataPage> result = new ArrayList<>();
        LeafNode leaf = findLeaf(start);

        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                K key = leaf.keys.get(i);
                if (key.compareTo(end) > 0) return result;

                if (key.compareTo(start) >= 0) {
                    PageManager.GlobalPageId gid = leaf.values.get(i);
                    // 通过 bufferPool 获取真实 DataPage
                    PageManager.DataPage page = DBMS.BUFFER_POOL.getPage(gid);
                    if (page != null) {
                        result.add(page);
                    }
                }
            }
            leaf = leaf.next;
        }

        return result;
    }


    /**
     * 插入 key -> 已有 GlobalPageId 的映射（通常用于索引构建）
     */
    public void insert(K key, PageManager.GlobalPageId gid) {
        LeafNode leaf = findLeaf(key);
        int idx = Collections.binarySearch(leaf.keys, key);
        if (idx >= 0) {
            // 已存在：覆盖映射（索引更新）
            leaf.values.set(idx, gid);
        } else {
            int insertPos = -idx - 1;
            leaf.keys.add(insertPos, key);
            leaf.values.add(insertPos, gid);
        }

        if (leaf.isOverflow()) splitLeaf(leaf);
    }

    /**
     * 便利方法：将数据写入 BufferPool 并把返回的 GlobalPageId 插入索引
     * - spaceId: 表空间 id
     * - pageNo: 你可以约定 pageNo 的分配方式；这里我们把传入的 pageNo 用作页号（或可修改为 auto-allocation）
     *
     * 注意：此方法假设 pageNo 唯一且由调用方/测试负责分配。
     */
    public void insertRecord(K key, byte[] record, int spaceId, int pageNo) {
        PageManager.DataPage page = new PageManager.DataPage(pageNo);
        page.addRecord(record);
        // 把数据写入缓冲池（并标记脏页）
        DBMS.BUFFER_POOL.putPage(page, spaceId);
        PageManager.GlobalPageId gid = new PageManager.GlobalPageId(spaceId, pageNo);
        insert(key, gid);
    }

    /**
     * 删除索引项（不物理删除 data page）
     */
    public void delete(K key) {
        LeafNode leaf = findLeaf(key);
        int idx = Collections.binarySearch(leaf.keys, key);
        if (idx < 0) return; // 不存在
        leaf.keys.remove(idx);
        leaf.values.remove(idx);

        if (leaf != root && leaf.isUnderflow()) {
            rebalanceAfterDelete(leaf);
        }
    }

    /**
     * 更新索引（将 key 对应的映射指向新的 GlobalPageId）
     */
    public void update(K key, PageManager.GlobalPageId newGid) {
        LeafNode leaf = findLeaf(key);
        int idx = Collections.binarySearch(leaf.keys, key);
        if (idx >= 0) leaf.values.set(idx, newGid);
        else throw new RuntimeException("Key not found: " + key);
    }

    /**
     * 范围查询，返回 DataPages（可能返回 null 元素若相应 gid 无法加载）
     */
    public List<PageManager.DataPage> rangeSearchPages(K start, K end) throws IOException {
        List<PageManager.DataPage> result = new ArrayList<>();
        LeafNode leaf = findLeaf(start);
        while (leaf != null) {
            for (int i = 0; i < leaf.keys.size(); i++) {
                K k = leaf.keys.get(i);
                if (k.compareTo(end) > 0) return result;
                if (k.compareTo(start) >= 0) {
                    PageManager.GlobalPageId gid = leaf.values.get(i);
                    if (gid != null) result.add(DBMS.BUFFER_POOL.getPage(gid));
                    else result.add(null);
                }
            }
            leaf = leaf.next;
        }
        return result;
    }

    // ========== 内部实现（分裂/查找/借合并等） ==========

    private LeafNode findLeaf(K key) {
        BPlusNode node = root;
        while (!node.isLeaf) {
            InternalNode in = (InternalNode) node;
            int idx = Collections.binarySearch(in.keys, key);
            int childIndex = idx >= 0 ? idx + 1 : -idx - 1;
            node = in.children.get(childIndex);
        }
        return (LeafNode) node;
    }

    private void splitLeaf(LeafNode leaf) {
        int mid = leaf.keys.size() / 2;

        LeafNode newLeaf = new LeafNode();
        newLeaf.keys.addAll(leaf.keys.subList(mid, leaf.keys.size()));
        newLeaf.values.addAll(leaf.values.subList(mid, leaf.values.size()));

        // 修剪原节点
        leaf.keys.subList(mid, leaf.keys.size()).clear();
        leaf.values.subList(mid, leaf.values.size()).clear();

        // 维护链表
        newLeaf.next = leaf.next;
        if (leaf.next != null) leaf.next.prev = newLeaf;
        leaf.next = newLeaf;
        newLeaf.prev = leaf;

        // parent 处理
        if (leaf == root) {
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(newLeaf.keys.get(0));
            newRoot.children.add(leaf);
            newRoot.children.add(newLeaf);
            root = newRoot;
            leaf.parent = newRoot;
            newLeaf.parent = newRoot;
        } else {
            insertIntoParent(leaf, newLeaf.keys.get(0), newLeaf);
        }
    }

    private void insertIntoParent(BPlusNode left, K key, BPlusNode right) {
        InternalNode parent = (InternalNode) left.parent;
        int idx = Collections.binarySearch(parent.keys, key);
        int insertPos = idx >= 0 ? idx + 1 : -idx - 1;
        parent.keys.add(insertPos, key);
        parent.children.add(insertPos + 1, right);
        right.parent = parent;
        if (parent.isOverflow()) splitInternal(parent);
    }

    private void splitInternal(InternalNode internal) {
        int mid = internal.keys.size() / 2;
        K upKey = internal.keys.get(mid);

        InternalNode newInternal = new InternalNode();
        newInternal.keys.addAll(internal.keys.subList(mid + 1, internal.keys.size()));
        newInternal.children.addAll(internal.children.subList(mid + 1, internal.children.size()));
        for (BPlusNode c : newInternal.children) c.parent = newInternal;

        internal.keys.subList(mid, internal.keys.size()).clear();
        internal.children.subList(mid + 1, internal.children.size()).clear();

        if (internal == root) {
            InternalNode newRoot = new InternalNode();
            newRoot.keys.add(upKey);
            newRoot.children.add(internal);
            newRoot.children.add(newInternal);
            root = newRoot;
            internal.parent = newRoot;
            newInternal.parent = newRoot;
        } else {
            insertIntoParent(internal, upKey, newInternal);
        }
    }

    // 叶子删除再平衡
    private void rebalanceAfterDelete(LeafNode leaf) {
        InternalNode parent = (InternalNode) leaf.parent;
        if (parent == null) return;

        int index = parent.children.indexOf(leaf);

        // 尝试借左
        if (index > 0) {
            LeafNode left = (LeafNode) parent.children.get(index - 1);
            if (left.keys.size() > order / 2) {
                K borrowedKey = left.keys.remove(left.keys.size() - 1);
                PageManager.GlobalPageId borrowedVal = left.values.remove(left.values.size() - 1);
                leaf.keys.add(0, borrowedKey);
                leaf.values.add(0, borrowedVal);
                parent.keys.set(index - 1, leaf.keys.get(0));
                return;
            }
        }

        // 尝试借右
        if (index < parent.children.size() - 1) {
            LeafNode right = (LeafNode) parent.children.get(index + 1);
            if (right.keys.size() > order / 2) {
                K borrowedKey = right.keys.remove(0);
                PageManager.GlobalPageId borrowedVal = right.values.remove(0);
                leaf.keys.add(borrowedKey);
                leaf.values.add(borrowedVal);
                parent.keys.set(index, right.keys.get(0));
                return;
            }
        }

        // 合并
        if (index > 0) {
            // 与左合并
            LeafNode left = (LeafNode) parent.children.get(index - 1);
            left.keys.addAll(leaf.keys);
            left.values.addAll(leaf.values);
            left.next = leaf.next;
            if (leaf.next != null) leaf.next.prev = left;
            parent.keys.remove(index - 1);
            parent.children.remove(index);

            if (parent == root && parent.keys.isEmpty()) {
                root = left;
                left.parent = null;
            } else if (parent.isUnderflow()) {
                rebalanceInternal(parent);
            }
        } else {
            // 与右合并
            LeafNode right = (LeafNode) parent.children.get(index + 1);
            leaf.keys.addAll(right.keys);
            leaf.values.addAll(right.values);
            leaf.next = right.next;
            if (right.next != null) right.next.prev = leaf;
            parent.keys.remove(index);
            parent.children.remove(index + 1);

            if (parent == root && parent.keys.isEmpty()) {
                root = leaf;
                leaf.parent = null;
            } else if (parent.isUnderflow()) {
                rebalanceInternal(parent);
            }
        }
    }

    // 内部节点再平衡（借或合并）
    private void rebalanceInternal(InternalNode internal) {
        InternalNode parent = (InternalNode) internal.parent;
        if (parent == null) {
            if (internal.keys.isEmpty()) {
                root = internal.children.get(0);
                root.parent = null;
            }
            return;
        }

        int index = parent.children.indexOf(internal);

        // 尝试借左
        if (index > 0) {
            InternalNode left = (InternalNode) parent.children.get(index - 1);
            if (left.keys.size() > order / 2) {
                K sepKey = parent.keys.get(index - 1);
                K borrowedKey = left.keys.remove(left.keys.size() - 1);
                BPlusNode borrowedChild = left.children.remove(left.children.size() - 1);
                borrowedChild.parent = internal;

                parent.keys.set(index - 1, borrowedKey);
                internal.keys.add(0, sepKey);
                internal.children.add(0, borrowedChild);
                return;
            }
        }

        // 尝试借右
        if (index < parent.children.size() - 1) {
            InternalNode right = (InternalNode) parent.children.get(index + 1);
            if (right.keys.size() > order / 2) {
                K sepKey = parent.keys.get(index);
                K borrowedKey = right.keys.remove(0);
                BPlusNode borrowedChild = right.children.remove(0);
                borrowedChild.parent = internal;

                internal.keys.add(sepKey);
                internal.children.add(borrowedChild);
                parent.keys.set(index, borrowedKey);
                return;
            }
        }

        // 合并：优先与左合并
        if (index > 0) {
            InternalNode left = (InternalNode) parent.children.get(index - 1);
            K sepKey = parent.keys.remove(index - 1);
            left.keys.add(sepKey);
            left.keys.addAll(internal.keys);
            for (BPlusNode c : internal.children) {
                left.children.add(c);
                c.parent = left;
            }
            parent.children.remove(index);
        } else {
            InternalNode right = (InternalNode) parent.children.get(index + 1);
            K sepKey = parent.keys.remove(index);
            internal.keys.add(sepKey);
            internal.keys.addAll(right.keys);
            for (BPlusNode c : right.children) {
                internal.children.add(c);
                c.parent = internal;
            }
            parent.children.remove(index + 1);
        }

        if (parent == root && parent.keys.isEmpty()) {
            root = parent.children.get(0);
            root.parent = null;
        } else if (parent.isUnderflow()) {
            rebalanceInternal(parent);
        }
    }

    // 打印树（按层）
    public void printTree() {
        Queue<BPlusNode> q = new LinkedList<>();
        q.add(root);
        while (!q.isEmpty()) {
            int sz = q.size();
            for (int i = 0; i < sz; i++) {
                BPlusNode node = q.poll();
                System.out.print(node.keys + " ");
                if (!node.isLeaf) {
                    q.addAll(((InternalNode) node).children);
                }
            }
            System.out.println();
        }
    }
}