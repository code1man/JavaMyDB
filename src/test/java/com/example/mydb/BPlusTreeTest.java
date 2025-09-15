package com.example.mydb;

import org.csu.mydb.storage.BPlusTree.BPlusTree;
import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.bufferPool.BufferPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BPlusTreeTest {
    private BPlusTree<Integer> tree;
    private BufferPool bufferPool;
    private AtomicInteger cacheHit = new AtomicInteger(0);
    private AtomicInteger cacheMiss = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        // 每次测试前清空“假磁盘”
        MockPageManager.reset();

        bufferPool = new BufferPool(150) {
            @Override
            public byte[] getPage(PageManager.GlobalPageId globalPageId) throws IOException {
                // 读锁优先检查缓存
                lock.readLock().lock();
                try {
                    PageManager.DataPage page = pageCache.get(globalPageId);
                    if (page != null) {
                        System.out.printf("[Thread-%s] Cache Hit %s%n",
                                Thread.currentThread().getName(), globalPageIdToString(globalPageId));
                        cacheHit.incrementAndGet();
                        return page;
                    }
                } finally {
                    lock.readLock().unlock();
                }

                // 写锁加载磁盘页（双重检查）
                lock.writeLock().lock();
                try {
                    PageManager.DataPage page = pageCache.get(globalPageId);
                    if (page != null) {
                        cacheHit.incrementAndGet();
                        return page;
                    }

                    System.out.printf("[Thread-%s] Cache Miss %s -> Load from Mock%n",
                            Thread.currentThread().getName(), globalPageIdToString(globalPageId));
                    page = MockPageManager.readPage(globalPageId.spaceId, globalPageId.pageNo);
                    pageCache.put(globalPageId, page);
                    cacheMiss.incrementAndGet();
                    return page;
                } finally {
                    lock.writeLock().unlock();
                }

            }

            @Override
            public void putPage(PageManager.DataPage page, int spaceId) {
                PageManager.GlobalPageId globalPageId = new PageManager.GlobalPageId(spaceId, page.getHeader().pageNo);
                lock.writeLock().lock();
                try {
                    pageCache.put(globalPageId, page);

                    if (page.getHeader().isDirty) {
                        markDirty(globalPageId);
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }

            @Override
            public void flush() throws IOException {
                lock.writeLock().lock();
                try {
                    for (PageManager.GlobalPageId globalPageId : dirtyPages.keySet()) {
                        PageManager.DataPage page = pageCache.get(globalPageId);
                        if (page != null) {
                            System.out.printf("[Thread-%s] Flush %s to Mock%n",
                                    Thread.currentThread().getName(), globalPageIdToString(globalPageId));
                            MockPageManager.writePage(page, globalPageId.spaceId);
                            page.getHeader().isDirty = false;
                            dirtyPages.remove(globalPageId);
                        }
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }

            private String globalPageIdToString(PageManager.GlobalPageId gid) {
                return String.format("Space-%d PageNo-%d", gid.spaceId, gid.pageNo);
            }
        };

        tree = new BPlusTree<>(4); // order=4
    }

    @Test
    void testInsertAndSearch() throws IOException {
        PageManager.GlobalPageId pid = new PageManager.GlobalPageId(1, 1);
        PageManager.DataPage page = MockPageManager.newPage(pid, "row1");
        bufferPool.putPage(page, pid.spaceId);
        tree.insert(10, pid);  // 插入 DataPage
        assertEquals(page, tree.search(10)); // 返回 DataPage
    }

    @Test
    void testRangeSearch() throws IOException {
        // 1. 插入 20 条数据
        for (int i = 1; i <= 10; i++) {
            PageManager.GlobalPageId pid = new PageManager.GlobalPageId(1, i);
            PageManager.DataPage page = MockPageManager.newPage(pid, "row" + i);
            bufferPool.putPage(page, pid.spaceId);
            tree.insert(i, pid);  // key = i, value = pid
        }

        for (int i = 11; i <= 20; i++) {
            PageManager.GlobalPageId pid = new PageManager.GlobalPageId(2, i - 10);
            PageManager.DataPage page = MockPageManager.newPage(pid, "row" + i);
            bufferPool.putPage(page, pid.spaceId);
            tree.insert(i, pid);  // key = i, value = pid
        }

        // 2. 范围查询，返回 GlobalPageId 列表
        List<PageManager.DataPage> results = tree.rangeSearch(5, 12);

        // 4. 验证结果
        assertEquals(8, results.size());
        assertEquals("row5", new String(results.get(0).getRecord(0)));
        assertEquals("row12", new String(results.get(7).getRecord(0)));
    }


    @Test
    void testDeleteBorrowAndMerge() throws IOException {
        for (int i = 1; i <= 10; i++) {
            PageManager.GlobalPageId pid = new PageManager.GlobalPageId(1, i);
            bufferPool.putPage(MockPageManager.newPage(pid, "row" + i), pid.spaceId);
            tree.insert(i, pid);
        }
        tree.delete(5);
        assertNull(tree.search(5));
        tree.delete(6);
        tree.delete(7);
        tree.delete(8);
        tree.delete(9);
        assertNotNull(tree.search(1));
        assertNotNull(tree.search(10));
    }

    @Test
    void testParentDegrade() throws IOException {
        // 1. 创建数据页并写入缓存
        for (int i = 1; i <= 4; i++) {
            PageManager.GlobalPageId pid = new PageManager.GlobalPageId(1, i);
            PageManager.DataPage page = MockPageManager.newPage(pid, "row" + i);
            bufferPool.putPage(page, pid.spaceId);  // 放入缓存
            tree.insert(i, pid);                     // 插入 B+ 树
        }

        // 2. 搜索测试
        assertNotNull(tree.search(1));
        PageManager.DataPage loaded1 = tree.search(1);
        assertEquals("row1", new String(loaded1.getRecord(0)));

        // 3. 删除触发父节点降级
        for (int i = 1; i <= 4; i++) {
            tree.delete(i);
        }

        // 4. 删除后搜索应该返回 null
        assertNull(tree.search(1));

        // 5. 打印树结构观察是否降级到叶节点
        tree.printTree();
    }


    @Test
    void testBufferPoolHitRate() throws IOException {
        PageManager.GlobalPageId pid = new PageManager.GlobalPageId(1, 100);
        PageManager.DataPage page = MockPageManager.newPage(pid, "test");
        bufferPool.putPage(page, pid.spaceId);
        bufferPool.getPage(pid);
        bufferPool.getPage(pid);
        assertEquals(2, cacheHit.get());
        assertEquals(0, cacheMiss.get());
    }
}

