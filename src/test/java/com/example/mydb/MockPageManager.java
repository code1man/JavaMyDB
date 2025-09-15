package com.example.mydb;

import org.csu.mydb.storage.PageManager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock 版本的 PageManager
 * - 内存 Map 代替磁盘存储
 * - 支持模拟“表不存在”“页不存在”
 */
public class MockPageManager {

    // 模拟多张表空间：spaceId → (pageNo → DataPage)
    private static final Map<Integer, ConcurrentHashMap<Integer, PageManager.DataPage>> fakeDisk = new ConcurrentHashMap<>();
    private static final AtomicInteger nextPageNo = new AtomicInteger(0);

    /**
     * 创建一个新的 DataPage 并写入一条数据
     */
    public static PageManager.DataPage newPage(PageManager.GlobalPageId pid, String data) {
        fakeDisk.putIfAbsent(pid.spaceId, new ConcurrentHashMap<>());
        PageManager.DataPage page = new PageManager.DataPage(pid.pageNo);
        page.addRecord(data.getBytes());
        fakeDisk.get(pid.spaceId).put(pid.pageNo, page);
        return page;
    }


    /**
     * 模拟写入磁盘
     */
    public static void writePage(PageManager.DataPage page, int spaceId) throws IOException {
        fakeDisk.putIfAbsent(spaceId, new ConcurrentHashMap<>());
        fakeDisk.get(spaceId).put(page.getHeader().pageNo, page);
        page.getHeader().isDirty = false;
        System.out.printf("[Mock] 写入 Space=%d PageNo=%d Data=%s%n",
                spaceId, page.getHeader().pageNo, new String(page.getRecord(0)));
    }

    /**
     * 模拟从磁盘读取
     */
    public static PageManager.DataPage readPage(int spaceId, int pageNo) throws IOException {
        if (!fakeDisk.containsKey(spaceId)) {
            throw new IOException("Table (spaceId) not found: " + spaceId);
        }
        PageManager.DataPage page = fakeDisk.get(spaceId).get(pageNo);
        if (page == null) {
            throw new IOException("Page not found: " + pageNo + " in spaceId " + spaceId);
        }
        System.out.printf("[Mock] 读取 Space=%d PageNo=%d Data=%s%n",
                spaceId, page.getHeader().pageNo, new String(page.getRecord(0)));
        return page;
    }

    /**
     * 清空“磁盘”
     */
    public static void reset() {
        fakeDisk.clear();
    }
}




