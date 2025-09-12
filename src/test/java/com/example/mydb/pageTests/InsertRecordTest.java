package com.example.mydb.pageTests;

import org.csu.mydb.storage.PageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.*;

public class InsertRecordTest {
    private PageManager pageManager;
    private static final String TEST_FILE = "G:\\coding_demo\\sql编译器代码\\TestFiles\\test.db";
    private static final int SPACE_ID = 1;
    private static final int ROOT_PAGE = 0; // 根页号

    @BeforeEach
    void setUp() throws IOException {
        pageManager = new PageManager();
        // 创建表空间
        pageManager.openFile(SPACE_ID, TEST_FILE);
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试文件
        pageManager.closeAllFiles();
//        new File(TEST_FILE).delete();
    }

    @Test
    void testInsertRecord() throws IOException {
// 1. 创建新表空间（文件）
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 验证文件已创建
        File file = new File(TEST_FILE);
        assertTrue(file.exists(), "表空间文件应已创建");

        // 3. 验证文件头页已初始化
        PageManager.DataPage headerPage = pageManager.readPageFromDisk(SPACE_ID, 0);
        assertNotNull(headerPage, "文件头页应存在");

        // 4. 验证页头信息
        PageManager.PageHeader header = headerPage.getHeader();
        assertEquals(0, header.getPageNo(), "文件头页号应为0");
        assertEquals(-1, header.getPrevPage(), "文件头页前向指针应为-1");
        assertEquals(-1, header.getNextPage(), "文件头页后向指针应为-1");
        assertEquals(0, header.getRecordCount(), "初始记录数应为0");
        assertEquals(PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE,
                header.getFreeSpace(), "初始空闲空间计算错误");

        // 5. 验证空闲页链表
        assertTrue(pageManager.getFreePageHeads().containsKey(SPACE_ID), "应有空闲页链表");
        assertEquals(-1, pageManager.getFreePageHeads().get(SPACE_ID), "初始空闲链表头应为-1");

        // 1. 准备测试数据
        String recordData = "User1,Alice,25";
        byte[] recordBytes = recordData.getBytes();

        // 2. 插入记录
        boolean success = pageManager.addRecord(SPACE_ID, ROOT_PAGE, recordBytes);
        assertTrue(success, "记录插入应成功");

        // 3. 获取根页
        PageManager.DataPage rootPage = pageManager.getPage(SPACE_ID, ROOT_PAGE);

        // 4. 验证页状态
        PageManager.PageHeader header1 = rootPage.getHeader();
        assertEquals(1, header1.getRecordCount(), "记录数应为1");
        assertEquals(1, header1.getSlotCount(), "槽位数应为1");
        assertEquals(0, header1.getFirstFreeSlot(), "第一个空闲槽位应为0（新槽位）");

        // 5. 计算预期空闲空间
        int recordSize = recordBytes.length;
        int slotSize = PageManager.SLOT_SIZE;
        int expectedFreeSpace = PageManager.PAGE_SIZE - PageManager.PAGE_HEADER_SIZE - recordSize - slotSize;
        assertEquals(expectedFreeSpace, header1.getFreeSpace(), "空闲空间计算错误");

        // 6. 验证槽位信息
        PageManager.Slot slot = rootPage.getSlots().get(0);
        assertEquals(PageManager.PAGE_SIZE - recordSize, slot.getOffset(), "记录偏移量错误");
        assertEquals(recordSize, slot.getLength(), "记录长度错误");
        assertEquals(1, slot.getStatus(), "槽位状态应为使用中");

        // 7. 验证记录内容
        byte[] retrieved = rootPage.getRecord(0);
        assertArrayEquals(recordBytes, retrieved, "读取的记录应与插入的记录一致");
    }
}