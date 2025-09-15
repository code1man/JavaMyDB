package org.csu.mydb.storage;

import org.csu.mydb.storage.BPlusTree.BPlusTree;

import java.io.RandomAccessFile;
import java.util.List;

public class Table<K extends Comparable<K>> {
    private final String name;             // 表名
    private final String path;             // 表文件路径
    private final List<String> columns;    // 列名
    private final int spaceId;             // 表空间 ID
    private final BPlusTree<K> primaryIndex; // 主键索引
    private final Class<K> keyClass; // 主键类型

    public Table(String name, String path, List<String> columns,
                 int spaceId, BPlusTree<K> primaryIndex,
                 // int primaryKeyIndex,
                 Class<K> keyClass) {
        this.name = name;
        this.path = path;
        this.columns = columns;
        this.spaceId = spaceId;
        this.primaryIndex = primaryIndex;
        //this.primaryKeyIndex = primaryKeyIndex;
        this.keyClass = keyClass;
    }

    public String getName() { return name; }
    public int getSpaceId() { return spaceId; }
    public List<String> getColumns() { return columns; }
    public BPlusTree<K> getPrimaryIndex() { return primaryIndex; }
    // public int getPrimaryKeyIndex() { return primaryKeyIndex; }
}


