package com.example.mydb;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.bufferPool.BufferPool;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class BufferPoolTest {

    private BufferPool bufferPool;
    private AtomicInteger cacheHit = new AtomicInteger(0);
    private AtomicInteger cacheMiss = new AtomicInteger(0);

    @BeforeEach
    void setUp() {
        // 每次测试前清空“假磁盘”
        MockPageManager.reset();

        bufferPool = new BufferPool(150, new PageManager()) {
            @Override
            public PageManager.DataPage getPage(PageManager.GlobalPageId globalPageId) throws IOException {
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
    }

    @Test
    void testPutAndGetPage() throws IOException {
        PageManager.DataPage page1 = new PageManager.DataPage(1);
        assertTrue(page1.addRecord("hello".getBytes()));

        bufferPool.putPage(page1, 0); // spaceId=0

        PageManager.DataPage cached = bufferPool.getPage(new PageManager.GlobalPageId(0, 1));
        assertNotNull(cached);
        assertEquals("hello", new String(cached.getRecord(0)));
    }

    @Test
    void testFlushAndReload() throws IOException {
        PageManager.DataPage page2 = new PageManager.DataPage(2);
        page2.addRecord("world".getBytes());
        bufferPool.putPage(page2, 0);

        bufferPool.flush();

        // 清空缓存，强制从“磁盘”读
        bufferPool.evictLRUPage();
        PageManager.DataPage reloaded = bufferPool.getPage(new PageManager.GlobalPageId(0, 2));
        assertNotNull(reloaded);
        assertEquals("world", new String(reloaded.getRecord(0)));
    }

    @Test
    void testConcurrentAccessWithStateLog() throws InterruptedException, IOException {
        // 先准备一页数据写入“磁盘”
        PageManager.DataPage page1 = new PageManager.DataPage(1);
        page1.addRecord("Alice".getBytes());
        MockPageManager.writePage(page1, 0);

        int threadCount = 3;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 1; i <= threadCount; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    System.out.printf("[Thread-%d] 准备获取 Space-0 PageNo=1%n", threadId);
                    Thread.sleep(200L * threadId); // 故意错开，制造竞争

                    System.out.printf("[Thread-%d] 正在尝试 getPage...%n", threadId);
                    PageManager.DataPage loaded = bufferPool.getPage(new PageManager.GlobalPageId(0, 1));

                    System.out.printf("[Thread-%d] 成功获取 Space-0 PageNo=1, Data=%s%n",
                            threadId, new String(loaded.getRecord(0)));
                } catch (IOException | InterruptedException e) {
                    System.out.printf("[Thread-%d] 异常: %s%n", threadId, e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();
    }

    @Test
    void testInitialWriteThenConcurrentReadWrite() throws InterruptedException, IOException {
        int initialRecords = 500;
        int threadCount = 2;
        int spaceCount = 3;

        // 1. 先批量写入 500 条数据到不同表空间
        for (int i = 1; i <= initialRecords; i++) {
            int spaceId = ThreadLocalRandom.current().nextInt(spaceCount); // 0~2
            PageManager.DataPage page = new PageManager.DataPage(i);
            page.addRecord(("Initial-Data-" + i).getBytes());
            bufferPool.putPage(page, spaceId);
        }

        bufferPool.flush();
        System.out.println("==== 初始写入完成 500 条数据 ====");

        // 2. 多线程随机读写
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        long startTime = System.currentTimeMillis();

        for (int t = 1; t <= threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < 500; i++) {
                        int spaceId = ThreadLocalRandom.current().nextInt(spaceCount);
                        int pageNo = ThreadLocalRandom.current().nextInt(1, initialRecords + 1);

                        // 读取
                        try {
                            PageManager.DataPage loaded = bufferPool.getPage(new PageManager.GlobalPageId(spaceId, pageNo));
                            loaded.getRecord(0);
                        } catch (IOException ignored) {
                        }

                        // 写入
                        PageManager.DataPage page = new PageManager.DataPage(pageNo);
                        page.addRecord(("Thread-" + threadId + "-Update-" + i).getBytes());
                        bufferPool.putPage(page, spaceId);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        long endTime = System.currentTimeMillis();
        System.out.println("==== 并发读写完成 ====");
        System.out.println("总耗时: " + (endTime - startTime) + " ms");
        int totalAccess = cacheHit.get() + cacheMiss.get();
        System.out.println("总访问次数: " + totalAccess);
        System.out.println("缓存命中: " + cacheHit.get());
        System.out.println("缓存未命中: " + cacheMiss.get());
        System.out.printf("缓存命中率: %.2f%%\n", cacheHit.get() * 100.0 / totalAccess);
    }
}

