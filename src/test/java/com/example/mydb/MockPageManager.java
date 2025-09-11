package com.example.mydb;

import org.csu.mydb.storage.PageManager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mock 版本的 PageManager
 * - 内存 Map 代替磁盘存储
 * - 支持模拟“表不存在”“页不存在”
 */
public class MockPageManager {

    // 模拟多张表空间：spaceId → (pageNo → DataPage)
    private static final Map<Integer, ConcurrentHashMap<Integer, PageManager.DataPage>> fakeDisk = new ConcurrentHashMap<>();

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




