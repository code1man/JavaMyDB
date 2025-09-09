package org.csu.mydb.storage;

import java.io.RandomAccessFile;
import java.util.List;

public class Table {
    private final String name;       // 表名
    private final String path;       // 表文件路径
    private final List<String> columns;  // 列名列表
    private RandomAccessFile fp;     // 文件指针（Java 中用 RandomAccessFile 替代 FILE*）

    public Table(String name, String path, List<String> columns) {
        this.name = name;
        this.path = path;
        this.columns = columns;
    }

    // Getter 方法
    public String getName() { return name; }
    public String getPath() { return path; }
    public List<String> getColumns() { return columns; }
    public RandomAccessFile getFp() { return fp; }
    public void setFp(RandomAccessFile fp) { this.fp = fp; }
}
