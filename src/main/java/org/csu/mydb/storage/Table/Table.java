package org.csu.mydb.storage.Table;

import org.csu.mydb.storage.BPlusTree.BPlusTree;
import org.csu.mydb.storage.Table.Column.Column;

import java.util.List;
import java.util.Stack;

public class Table{
    private final String name;             // 表名
    private final String path;             // 表文件路径
    private final List<Column> columns;    // 列名
    private final int spaceId;             // 表空间 ID
    private final BPlusTree primaryIndex; // 主键索引

    public Table(String name, String path, List<Column> columns,
                 int spaceId, BPlusTree primaryIndex) {
        this.name = name;
        this.path = path;
        this.columns = columns;
        this.spaceId = spaceId;
        this.primaryIndex = primaryIndex;
    }

    public String getName() { return name; }
    public int getSpaceId() { return spaceId; }
    public List<Column> getColumns() { return columns; }
    public BPlusTree getPrimaryIndex() { return primaryIndex; }

    public BPlusTree getTree() {
        return primaryIndex;
    }
    // public int getPrimaryKeyIndex() { return primaryKeyIndex; }
}


