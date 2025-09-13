package org.csu.mydb.storage;

import org.csu.mydb.storage.BPlusTree.BPlusTree;

import java.io.RandomAccessFile;
import java.util.List;

public class Table {
    private final String name;             // 表名
    private final String path;             // 表文件路径
    private final List<String> columns;    // 列名
    private final int spaceId;             // 表空间 ID
    private final BPlusTree<Integer> primaryIndex; // 主键索引
    private final int primaryKeyIndex;     // 主键列下标

    public Table(String name, String path, List<String> columns,
                 int spaceId, BPlusTree<Integer> primaryIndex,
                 int primaryKeyIndex) {
        this.name = name;
        this.path = path;
        this.columns = columns;
        this.spaceId = spaceId;
        this.primaryIndex = primaryIndex;
        this.primaryKeyIndex = primaryKeyIndex;
    }

    public String getName() { return name; }
    public int getSpaceId() { return spaceId; }
    public List<String> getColumns() { return columns; }
    public BPlusTree<Integer> getPrimaryIndex() { return primaryIndex; }
    public int getPrimaryKeyIndex() { return primaryKeyIndex; }
}


