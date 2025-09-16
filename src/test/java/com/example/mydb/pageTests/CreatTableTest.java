package com.example.mydb.pageTests;

import org.csu.mydb.storage.PageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class CreatTableTest {
    private PageManager pageManager;
    private static final String TEST_FILE = "G:\\coding_demo\\sql编译器代码\\TestFiles\\test.db";
    private static final int SPACE_ID = 1;

    @BeforeEach
    void setUp() {
        pageManager = new PageManager();
    }

    @AfterEach
    void tearDown() throws IOException {
        // 清理测试文件
//        pageManager.closeAllFiles();
//        new File(TEST_FILE).delete();
    }

    @Test
    void testCreateNewTableSpace() throws IOException {
        // 1. 创建新表空间（文件）
        pageManager.openFile(SPACE_ID, TEST_FILE);

        // 2. 验证文件已创建
        File file = new File(TEST_FILE);
        assertTrue(file.exists(), "表空间文件应已创建");

        // 3. 验证文件头页已初始化
        PageManager.Page headerPage = pageManager.readPage(SPACE_ID, 0);
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
    }
}
