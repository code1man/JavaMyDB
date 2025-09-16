package com.example.mydb;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.bufferPool.BufferPool;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class StorageSystemTest {
    private StorageSystem storageSystem;
    private PageManager pageManager;
    private BufferPool bufferPool;

    private static final String TEST_FILE = "G:\\coding_demo\\sql编译器代码\\TestFiles\\test.db";
    private static final int SPACE_ID = 1;
    private static final int ROOT_PAGE = 0;

    @BeforeEach
    void setUp() {
        storageSystem = new StorageSystem();
        pageManager = storageSystem.getPageManager();
        bufferPool = storageSystem.getBufferPool();
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试文件
        pageManager.closeAllFiles();
        Files.deleteIfExists(new File(TEST_FILE).toPath());
    }

    // ====================== 基础功能测试 ======================

    /**
     * 测试1: 创建表空间
     * 验证点:
     * 1. 文件成功创建
     * 2. 文件大小正确
     * 3. 文件头页正确初始化
     */
    @Test
    void testCreateTableSpace() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 验证文件已创建
        File file = new File(TEST_FILE);
        assertTrue(file.exists(), "表空间文件应已创建");
        assertEquals(PageManager.PAGE_SIZE, file.length(), "文件大小应为PAGE_SIZE");

        // 3. 验证文件头页
        PageManager.Page headerPage = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        PageManager.PageHeader header = headerPage.getHeader();

        assertEquals(0, header.getPageNo(), "文件头页号应为0");
        assertEquals(-1, header.getPrevPage(), "文件头页前向指针应为-1");
        assertEquals(-1, header.getNextPage(), "文件头页后向指针应为-1");
        assertEquals(0, header.getRecordCount(), "初始记录数应为0");
        assertEquals(PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE,
                header.getFreeSpace(), "初始空闲空间计算错误");
    }

    /**
     * 测试2: 分配新页
     * 验证点:
     * 1. 新页号正确
     * 2. 文件大小扩展
     * 3. 新页正确初始化
     */
    @Test
    void testAllocatePage() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 分配新页
        int newPageNo = pageManager.allocatePage(SPACE_ID);
        assertEquals(1, newPageNo, "新页号应为1");

        // 3. 验证文件大小
        File file = new File(TEST_FILE);
        assertEquals(PageManager.PAGE_SIZE * 2, file.length(), "文件大小应为2页");

        // 4. 验证新页状态
        PageManager.Page newPage = pageManager.getPage(SPACE_ID, newPageNo);
        PageManager.PageHeader header = newPage.getHeader();

        assertEquals(newPageNo, header.getPageNo(), "页号应匹配");
        assertEquals(0, header.getRecordCount(), "新页记录数应为0");
        assertEquals(PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE,
                header.getFreeSpace(), "新页空闲空间计算错误");
    }

    // ====================== 记录操作测试 ======================

    /**
     * 测试3: 插入和读取记录
     * 验证点:
     * 1. 插入成功
     * 2. 记录数增加
     * 3. 空闲空间减少
     * 4. 读取记录内容正确
     */
    @Test
    void testInsertAndRetrieveRecord() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 准备测试数据
        String recordData = "User1,Alice,25";
        byte[] recordBytes = recordData.getBytes();

        // 3. 插入记录
        boolean success = pageManager.addRecord(SPACE_ID, ROOT_PAGE, recordBytes);
        assertTrue(success, "记录插入应成功");

        // 4. 获取根页
        PageManager.Page rootPage = pageManager.getPage(SPACE_ID, ROOT_PAGE);

        // 5. 验证页状态
        PageManager.PageHeader header = rootPage.getHeader();
        assertEquals(1, header.getRecordCount(), "记录数应为1");
        assertEquals(1, header.getSlotCount(), "槽位数应为1");

        // 6. 计算预期空闲空间
        int recordSize = recordBytes.length;
        int slotSize = PageManager.SLOT_SIZE;
        int expectedFreeSpace = PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE - recordSize - slotSize;
        assertEquals(expectedFreeSpace, header.getFreeSpace(), "空闲空间计算错误");

        // 7. 验证槽位信息
        PageManager.Slot slot = rootPage.getSlots().get(0);
        assertEquals(PageManager.PAGE_SIZE - recordSize, slot.getOffset(), "记录偏移量错误");
        assertEquals(recordSize, slot.getLength(), "记录长度错误");
        assertEquals(1, slot.getStatus(), "槽位状态应为使用中");

        // 8. 验证记录内容
        byte[] retrieved = rootPage.getRecord(0);
        assertArrayEquals(recordBytes, retrieved, "读取的记录应与插入的记录一致");
    }

    /**
     * 测试4: 更新记录
     * 验证点:
     * 1. 更新成功
     * 2. 记录内容正确更新
     * 3. 空间使用正确
     */
    @Test
    void testUpdateRecord() throws IOException {
        // 1. 创建表空间并插入初始记录
        pageManager.openFile(SPACE_ID, TEST_FILE);
        String initialData = "User1,Alice,25";
        byte[] initialBytes = initialData.getBytes();
        pageManager.addRecord(SPACE_ID, ROOT_PAGE, initialBytes);

        // 2. 准备更新数据
        String updatedData = "User1,Alice,26";
        byte[] updatedBytes = updatedData.getBytes();

        // 3. 更新记录
        boolean success = pageManager.updateRecord(SPACE_ID, ROOT_PAGE, 0, updatedBytes);
        assertTrue(success, "记录更新应成功");

        // 4. 验证更新
        PageManager.Page page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        byte[] retrieved = page.getRecord(0);
        assertArrayEquals(updatedBytes, retrieved, "更新后的记录内容应匹配");

        // 5. 验证空间使用
        PageManager.PageHeader header = page.getHeader();
        if (updatedBytes.length > initialBytes.length) {
            int spaceDiff = updatedBytes.length - initialBytes.length;
            int expectedFreeSpace = PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE
                    - PageManager.SLOT_SIZE - updatedBytes.length;
            assertEquals(expectedFreeSpace, header.getFreeSpace(), "更新后空闲空间计算错误");
        }
    }

    /**
     * 测试5: 删除记录
     * 验证点:
     * 1. 删除成功
     * 2. 记录数减少
     * 3. 槽位状态更新
     * 4. 空闲链表更新
     */
    @Test
    void testDeleteRecord() throws IOException {
        // 1. 创建表空间并插入记录
        pageManager.openFile(SPACE_ID, TEST_FILE);
        String recordData = "User1,Alice,25";
        byte[] recordBytes = recordData.getBytes();
        pageManager.addRecord(SPACE_ID, ROOT_PAGE, recordBytes);

        // 2. 删除记录
        boolean success = pageManager.freeRecord(SPACE_ID, ROOT_PAGE, 0);
        assertTrue(success, "记录删除应成功");

        // 3. 验证页状态
        PageManager.Page page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        PageManager.PageHeader header = page.getHeader();
        assertEquals(0, header.getRecordCount(), "记录数应为0");

        // 4. 验证槽位状态
        PageManager.Slot slot = page.getSlots().get(0);
        assertEquals(0, slot.getStatus(), "槽位状态应为空闲");

        // 5. 验证空闲链表
        assertEquals(0, header.getFirstFreeSlot(), "第一个空闲槽位应为0");
    }

    // ====================== 缓存管理测试 ======================

    /**
     * 测试6: 缓存命中测试
     * 验证点:
     * 1. 首次获取页缓存未命中
     * 2. 再次获取同一页缓存命中
     */
    @Test
    void testCacheHit() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 首次获取页（应缓存未命中）
        PageManager.Page page1 = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        assertNotNull(page1, "应获取到页");

        // 3. 再次获取同一页（应缓存命中）
        PageManager.Page page2 = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        assertSame(page1, page2, "应返回相同的页对象（缓存命中）");
    }

    /**
     * 测试7: 脏页刷回测试
     * 验证点:
     * 1. 修改后页标记为脏页
     * 2. 刷回后磁盘内容更新
     * 3. 刷回后脏页标志清除
     */
    @Test
    void testDirtyPageFlush() throws IOException {
        // 1. 创建表空间并插入记录
        pageManager.openFile(SPACE_ID, TEST_FILE);
        String recordData = "User1,Alice,25";
        byte[] recordBytes = recordData.getBytes();
        pageManager.addRecord(SPACE_ID, ROOT_PAGE, recordBytes);

        // 2. 获取页并验证脏页状态
        PageManager.Page page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        assertTrue(page.getHeader().isDirty(), "修改后页应为脏页");

        // 3. 手动刷回脏页
        bufferPool.flush();

        // 4. 验证脏页标志清除
        assertFalse(page.getHeader().isDirty(), "刷回后脏页标志应清除");

        // 5. 从磁盘重新加载验证
        PageManager.Page diskPage = pageManager.readPage(SPACE_ID, ROOT_PAGE);
        byte[] retrieved = diskPage.getRecord(0);
        assertArrayEquals(recordBytes, retrieved, "磁盘上的记录应与内存一致");
    }

    // ====================== 边界和异常测试 ======================

    /**
     * 测试8: 大记录测试
     * 验证点:
     * 1. 接近页大小的记录可以插入
     * 2. 超过页大小的记录插入失败
     */
    @Test
    void testLargeRecord() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 创建接近页大小的记录
        int maxRecordSize = PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE - PageManager.SLOT_SIZE;
        byte[] largeRecord = new byte[maxRecordSize];
        Arrays.fill(largeRecord, (byte) 'A');

        // 3. 插入大记录
        boolean success = pageManager.addRecord(SPACE_ID, ROOT_PAGE, largeRecord);
        assertTrue(success, "大记录插入应成功");

        // 4. 创建超过页大小的记录
        byte[] oversizedRecord = new byte[maxRecordSize + 1];

        // 5. 尝试插入超大记录
        success = pageManager.addRecord(SPACE_ID, ROOT_PAGE, oversizedRecord);
        assertFalse(success, "超大记录插入应失败");
    }

    /**
     * 测试9: 多记录测试
     * 验证点:
     * 1. 多记录插入成功
     * 2. 记录数正确
     * 3. 槽位数正确
     */
    @Test
    void testMultipleRecords() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 插入多条记录
        int recordCount = 10;
        for (int i = 0; i < recordCount; i++) {
            String record = "Record" + i;
            boolean success = pageManager.addRecord(SPACE_ID, ROOT_PAGE, record.getBytes());
            assertTrue(success, "记录" + i + "插入应成功");
        }

        // 3. 验证页状态
        PageManager.Page page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        PageManager.PageHeader header = page.getHeader();

        assertEquals(recordCount, header.getRecordCount(), "记录数应为" + recordCount);
        assertEquals(recordCount, header.getSlotCount(), "槽位数应为" + recordCount);

        // 4. 验证所有记录
        for (int i = 0; i < recordCount; i++) {
            byte[] expected = ("Record" + i).getBytes();
            byte[] actual = page.getRecord(i);
            assertArrayEquals(expected, actual, "记录" + i + "内容不匹配");
        }
    }

    /**
     *  : 页回收测试
     * 验证点:
     * 1. 释放页成功
     * 2. 页被添加到空闲链表
     * 3. 新分配的页重用空闲页
     */
    @Test
    void testPageRecycling() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 分配新页
        int pageNo = pageManager.allocatePage(SPACE_ID);

        // 3. 释放页
        pageManager.freePage(SPACE_ID, pageNo);

        // 4. 验证空闲链表
        int freePageHead = pageManager.getFreePageHeads().get(SPACE_ID);
        assertEquals(pageNo, freePageHead, "空闲链表头应为释放的页");

        // 5. 再次分配页（应重用）
        int newPageNo = pageManager.allocatePage(SPACE_ID);
        assertEquals(pageNo, newPageNo, "新分配的页应重用空闲页");
    }

    // ====================== 高级功能测试 ======================

    /**
     * 测试11: 事务一致性测试
     * 验证点:
     * 1. 未提交的修改不持久化
     * 2. 提交后修改持久化
     */
//    @Test
//    void testTransactionConsistency() throws IOException {
//        // 1. 创建表空间
//        pageManager.openFile(SPACE_ID, TEST_FILE);
//
//        // 2. 插入记录但不提交
//        String recordData = "TempRecord";
//        byte[] recordBytes = recordData.getBytes();
//        pageManager.addRecord(SPACE_ID, ROOT_PAGE, recordBytes);
//
//        // 3. 模拟崩溃（不刷回脏页）
//        pageManager.closeAllFiles();
//
//        // 4. 重新打开文件
//        pageManager.openFile(SPACE_ID, TEST_FILE);
//
//        // 5. 验证记录不存在（未提交）
//        PageManager.Page page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
//        assertEquals(0, page.getHeader().getRecordCount(), "未提交的记录不应存在");
//
//        // 6. 插入记录并提交
//        pageManager.addRecord(SPACE_ID, ROOT_PAGE, recordBytes);
//        bufferPool.flush(); // 提交事务
//
//        // 7. 重新打开文件
//        pageManager.closeAllFiles();
//        pageManager.openFile(SPACE_ID, TEST_FILE);
//
//        // 8. 验证记录存在（已提交）
//        page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
//        assertEquals(1, page.getHeader().getRecordCount(), "已提交的记录应存在");
//        assertArrayEquals(recordBytes, page.getRecord(0), "记录内容应匹配");
//    }

    /**
     * 测试12: 缓存淘汰测试
     * 验证点:
     * 1. 缓存满时淘汰最近最少使用的页
     * 2. 脏页淘汰前刷回
     */
//    @Test
//    void testCacheEviction() throws IOException {
//        // 创建小缓存池（2页）
//        BufferPool smallBufferPool = new BufferPool(2, pageManager);
//        pageManager.setBufferPool(smallBufferPool);
//
//        // 1. 创建表空间
//        pageManager.openFile(SPACE_ID, TEST_FILE);
//
//        // 2. 分配两页
//        int page1 = pageManager.allocatePage(SPACE_ID);
//        int page2 = pageManager.allocatePage(SPACE_ID);
//
//        // 3. 获取页填充缓存
//        PageManager.Page p1 = pageManager.getPage(SPACE_ID, page1);
//        PageManager.Page p2 = pageManager.getPage(SPACE_ID, page2);
//
//        // 4. 修改页1使其成为脏页
//        String recordData = "DirtyPage";
//        p1.addRecord(recordData.getBytes());
//
//        // 5. 分配第三页（触发缓存淘汰）
//        int page3 = pageManager.allocatePage(SPACE_ID);
//        PageManager.Page p3 = pageManager.getPage(SPACE_ID, page3);
//
//        // 6. 验证页1已刷回
//        PageManager.Page diskPage = pageManager.readPageFromDisk(SPACE_ID, page1);
//        assertEquals(1, diskPage.getHeader().getRecordCount(), "脏页应已刷回");
//    }

    // ====================== 性能测试 ======================

    /**
     * 测试13: 批量操作性能测试
     * 验证点:
     * 1. 批量操作性能可接受
     * 2. 缓存减少磁盘I/O
     */
    @Test
    void testBulkOperationsPerformance() throws IOException {
        // 1. 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);

        int recordCount = 100;
        long startTime = System.currentTimeMillis();

        // 2. 批量插入记录
        for (int i = 0; i < recordCount; i++) {
            String record = "Record" + i;
            pageManager.addRecord(SPACE_ID, ROOT_PAGE, record.getBytes());
        }

        long insertTime = System.currentTimeMillis() - startTime;
        System.out.println("插入" + recordCount + "条记录耗时: " + insertTime + "ms");

        // 3. 批量读取记录
        startTime = System.currentTimeMillis();
        PageManager.Page page = pageManager.getPage(SPACE_ID, ROOT_PAGE);
        for (int i = 0; i < recordCount; i++) {
            byte[] record = page.getRecord(i);
            assertNotNull(record, "记录" + i + "应存在");
        }

        long readTime = System.currentTimeMillis() - startTime;
        System.out.println("读取" + recordCount + "条记录耗时: " + readTime + "ms");

        // 4. 验证性能可接受（可根据实际情况调整阈值）
        assertTrue(insertTime < 1000, "插入性能应在可接受范围内");
        assertTrue(readTime < 100, "读取性能应在可接受范围内");
    }

    @Test
    void createTable_Success(@TempDir Path tempDir) {


        // 1. 准备测试参数
        String filePath = tempDir.resolve("test_table.ibd").toString();
        List<Column> columns = createTestColumns();

        // 2. 调用方法
        int spaceId = StorageSystem.createTable(filePath, columns);

        // 3. 简单验证
        assertTrue(spaceId > 0, "返回的spaceId应该大于0");
        assertTrue(new File(filePath).exists(), "表文件应该被创建");
    }

    private List<Column> createTestColumns() {
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", "INT", 4, 0, , true, false, null));
        columns.add(new Column("name", "VARCHAR", 50, 0, , false, true, null));
        columns.add(new Column("email", "VARCHAR", 100, 0, , false, true, null));
        return columns;
    }
}
