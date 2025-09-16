package com.example.mydb;

import org.csu.mydb.storage.BPlusTree.BPlusTree;
import org.csu.mydb.storage.StorageSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;

import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;

import java.util.Arrays;


public class BPlusTreeTest {
    private BPlusTree tree;

    @BeforeEach
    public void setUp() throws IOException {
        // 模拟 PageManager (你需要保证有一个能跑的实现)
        StorageSystem storageSystem = new StorageSystem();

        // 定义表结构：一个主键 id(int)，一个 name(string)
        Column idCol = new Column("id", "INT", 4,0,0,true, true, null);   // true 表示是主键
        Column nameCol = new Column("name", "VARCHAR", 10, 0,1, false, false, null);

        List<Column> columns = Arrays.asList(idCol, nameCol);
        int spaceId = StorageSystem.createTable("G:\\MyDB\\MyDB\\src\\main\\resources\\test\\jb.idb", columns);
        // order = 3
        tree = new BPlusTree(3, spaceId, storageSystem, columns);
    }

    @Test
    public void testInsertAndSearch() throws IOException {
        // 插入几条数据
        tree.insert(Arrays.asList(1, "Alice"));
        tree.insert(Arrays.asList(2, "Bob"));
        tree.insert(Arrays.asList(3, "Charlie"));

        // 查找
        List<Object> row = tree.search(new Key(List.of(2), tree.getColumns()));
        assertNotNull(row);
        assertEquals("Bob", row.get(1));
    }

    @Test
    public void testUpdate() throws IOException {
        tree.insert(Arrays.asList(1, "Alice"));
        tree.insert(Arrays.asList(2, "Bob"));

        // 更新 Bob -> Bobby
        boolean updated = tree.update(new Key(Arrays.asList(2), tree.getColumns()), Arrays.asList(2, "Bobby"));
        assertTrue(updated);

        List<Object> row = tree.search(new Key(Arrays.asList(2), tree.getColumns()));
        assertEquals("Bobby", row.get(1));
    }

    @Test
    public void testDelete() throws IOException {
        tree.insert(Arrays.asList(1, "Alice"));
        tree.insert(Arrays.asList(2, "Bob"));
        tree.insert(Arrays.asList(3, "Charlie"));

        boolean removed = tree.delete(new Key(Arrays.asList(2), tree.getColumns()));
        assertTrue(removed);

        List<Object> row = tree.search(new Key(Arrays.asList(2), tree.getColumns()));
        assertNull(row); // 已经删掉了
    }

    @Test
    public void testBulkInsert() throws IOException {
        for (int i = 1; i <= 50; i++) {
            tree.insert(Arrays.asList(i, "Name" + i));
        }

        List<Object> row = tree.search(new Key(List.of(3), tree.getColumns()));
        assertNotNull(row);
        assertEquals("Name3", row.get(1));
    }
}


