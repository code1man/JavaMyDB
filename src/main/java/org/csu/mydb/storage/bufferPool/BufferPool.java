package org.csu.mydb.storage.bufferPool;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.disk.DiskAccessor;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BufferPool {
    protected final int poolSize;

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final LRUCache<PageManager.GlobalPageId, PageManager.Page> pageCache;
    protected final ConcurrentHashMap<PageManager.GlobalPageId, DirtyPageNode> dirtyPages;


    protected final float FLUSH_THRESHOLD_RATIO = 0.1f;

    private final DiskAccessor diskAccessor;

    // 脏页链表
    public static class DirtyPageNode {
        PageManager.GlobalPageId pageId;
        long lastModifiedTime;
        DirtyPageNode prev, next;
        DirtyPageNode(PageManager.GlobalPageId pageId, long time) {
            this.pageId = pageId;
            this.lastModifiedTime = time;
        }
    }
    private final DirtyPageNode dirtyHead = new DirtyPageNode(null, -1);
    private final DirtyPageNode dirtyTail = new DirtyPageNode(null, -1);
    protected int dirtyCount = 0;

    public BufferPool(int poolSize, DiskAccessor diskAccessor) {
        this.poolSize = poolSize;
        this.pageCache = new LRUCache<>(poolSize);
        this.dirtyPages = new ConcurrentHashMap<>();
        this.diskAccessor = diskAccessor;
        dirtyHead.next = dirtyTail;
        dirtyTail.prev = dirtyHead;
    }

    // 获取页（优先缓存，未命中读磁盘）
    public PageManager.Page getPage(PageManager.GlobalPageId pageId) throws IOException {
        lock.readLock().lock();
        try {
            PageManager.Page page = pageCache.get(pageId);
            if (page != null) return page;
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            PageManager.Page page = pageCache.get(pageId);
            if (page != null) return page;

            page = diskAccessor.readPage(pageId.spaceId, pageId.pageNo);
            if (page != null) {
                pageCache.put(pageId, page);
            }
            return page;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 写入页并标记脏页
    public void putPage(PageManager.Page page, int spaceId) {
        PageManager.GlobalPageId pageId = new PageManager.GlobalPageId(spaceId, page.getHeader().pageNo);
        lock.writeLock().lock();
        try {
            pageCache.put(pageId, page);

            if (page.getHeader().isDirty) {
                markDirty(pageId);
                if (dirtyCount > poolSize * FLUSH_THRESHOLD_RATIO) {
                    flushBatch(dirtyCount / 2); // 一次刷一半脏页
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 全量刷盘
    public void flush() throws IOException {
        lock.writeLock().lock();
        try {
            DirtyPageNode cur = dirtyHead.next;
            while (cur != dirtyTail) {
                PageManager.GlobalPageId pageId = cur.pageId;
                PageManager.Page page = pageCache.get(pageId);
                if (page != null) {
//                    PageManager.writePage(page, pageId.spaceId);
                    diskAccessor.writePage(pageId.spaceId, pageId.pageNo, page);
                    page.getHeader().isDirty = false;
                }
                dirtyPages.remove(pageId);
                cur = cur.next;
            }
            dirtyHead.next = dirtyTail;
            dirtyTail.prev = dirtyHead;
            dirtyCount = 0;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 淘汰最久未使用的页
    public void evictLRUPage() throws IOException {
        lock.writeLock().lock();
        try {
            PageManager.GlobalPageId lruPageId = pageCache.getEldestKey();
            if (lruPageId != null) {
                PageManager.Page page = pageCache.get(lruPageId);
                if (page != null && dirtyPages.containsKey(lruPageId)) {
//                    PageManager.writePage(page, lruPageId.spaceId);
                    diskAccessor.writePage(lruPageId.spaceId, lruPageId.pageNo, page);
                    dirtyPages.remove(lruPageId);
                    page.getHeader().isDirty = false;
                    dirtyCount--;
                }
                pageCache.evict();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 标记脏页：移动到链表尾
    protected void markDirty(PageManager.GlobalPageId pageId) {
        DirtyPageNode node = dirtyPages.get(pageId);
        if (node != null) {
            // 已存在 → 先移除再插入尾部
            node.prev.next = node.next;
            node.next.prev = node.prev;
        } else {
            node = new DirtyPageNode(pageId, System.currentTimeMillis());
            dirtyPages.put(pageId, node);
            dirtyCount++;
        }
        // 插入尾部
        node.prev = dirtyTail.prev;
        node.next = dirtyTail;
        dirtyTail.prev.next = node;
        dirtyTail.prev = node;
    }

    // 批量刷盘（按链表顺序）
    public void flushBatch(int batchSize) throws IOException {
        int flushed = 0;
        DirtyPageNode cur = dirtyHead.next;
        while (cur != dirtyTail && flushed < batchSize) {
            PageManager.GlobalPageId pageId = cur.pageId;
            PageManager.Page page = pageCache.get(pageId);
            if (page != null) {
//                PageManager.writePage(page, pageId.spaceId);
                diskAccessor.writePage(pageId.spaceId, pageId.pageNo, page);
                page.getHeader().isDirty = false;
            }
            dirtyPages.remove(pageId);
            cur.prev.next = cur.next;
            cur.next.prev = cur.prev;
            dirtyCount--;
            flushed++;
            cur = cur.next;
        }
    }
}

