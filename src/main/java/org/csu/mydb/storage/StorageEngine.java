package org.csu.mydb.storage;

import org.csu.mydb.storage.BPlusTree.BPlusTree;
import org.csu.mydb.storage.Table.Table;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 存储引擎：负责数据库的创建、删除、打开、关闭，以及表的管理。
 */
public class StorageEngine {
    private String prePath = "";       // 当前数据库路径（如 "save/repos/db1/"）
    private boolean isOpen = false;    // 是否已打开数据库
    private final Map<String, Table> tables = new HashMap<>(); // 当前打开的所有表
    private final PageManager pageManager;

    public StorageEngine(PageManager pageManager) {
        this.pageManager = pageManager;
    }

    /**
     * 打开数据库
     */
    public void myOpenDataBase(String dbName) throws IOException {
        File dbDir = new File("save/repos/" + dbName);
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            System.out.println("数据库不存在: " + dbName);
            return;
        }
        prePath = dbDir.getPath() + "/";
        isOpen = true;

        tables.clear();
        for (File file : Objects.requireNonNull(dbDir.listFiles())) {
            if (file.getName().endsWith(".ibd")) {
                String tableName = file.getName().replace(".ibd", "");
                // TODO: 从系统表加载表的元信息（列定义、主键等）
                List<Column> columns = new ArrayList<>();
//                int spaceId = pageManager.getSpaceId(file.getPath());
//                BPlusTree tree = new BPlusTree(3, spaceId, pageManager, columns);
//                tables.put(tableName, new Table(tableName, file.getPath(), columns, tree));
            }
        }
        System.out.println("打开数据库成功: " + dbName);
    }

    /**
     * 关闭数据库
     */
    public void myCloseDataBase() {
        tables.clear();
        prePath = "";
        isOpen = false;
        System.out.println("关闭数据库成功");
    }

    /**
     * 创建数据库
     */
    public void myCreateDataBase(String dataBaseName) {
        String path = "save/repos/" + dataBaseName;
        File dbDir = new File(path);
        if (dbDir.exists()) {
            System.out.println("该数据库已存在");
            return;
        }
        if (dbDir.mkdirs()) {
            System.out.println("创建数据库成功: " + dataBaseName);
        } else {
            System.out.println("创建数据库失败（权限不足或路径无效）");
        }
    }

    /**
     * 删除数据库
     */
    public void myDropDataBase(String dataBaseName) {
        String path = "save/repos/" + dataBaseName;
        File dbDir = new File(path);
        if (!dbDir.exists()) {
            System.out.println("该数据库不存在");
            return;
        }
        if (deleteDirectory(dbDir)) {
            System.out.println("删除数据库成功: " + dataBaseName);
        } else {
            System.out.println("删除数据库失败（存在无法删除的文件）");
        }
    }

    /**
     * 创建表（新建 .ibd 文件 + 初始化 B+ 树）
     */
    public void myCreateTable(String tableName, List<String> columns) throws IOException {
        if (!isOpen) {
            System.out.println("无选中数据库，请先打开数据库");
            return;
        }
        String tablePath = prePath + tableName + ".ibd";
        File tableFile = new File(tablePath);
        if (tableFile.exists()) {
            System.out.println("该表已经存在!");
            return;
        }
        // 初始化一个表空间
//        int spaceId = pageManager.createSpace(tablePath);
//        BPlusTree tree = new BPlusTree(3, spaceId, pageManager, columns);
//        tables.put(tableName, new Table(tableName, tablePath, columns, tree));
        System.out.println("创建新表成功: " + tableName);
    }

    /**
     * 删除表
     */
    public void myDropTable(String tableName) {
        if (!isOpen) {
            System.out.println("无选中数据库，请先打开数据库");
            return;
        }
        String tablePath = prePath + tableName + ".ibd";
        File tableFile = new File(tablePath);

        if (!tables.containsKey(tableName)) {
            System.out.println("该表不存在!");
            return;
        }
        tables.remove(tableName);

        if (tableFile.delete()) {
            System.out.println("删除表成功: " + tableName);
        } else {
            System.out.println("删除表失败（文件被占用或无权限）");
        }
    }

    /**
     * 插入数据
     */
    public void myInsert(String tableName, List<String> values) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            System.out.println("该表不存在!");
            return;
        }
        table.getTree().insert(Collections.singletonList(values));
        System.out.println("插入成功 -> 表: " + tableName + " 值: " + values);
    }

    /**
     * 删除数据
     */
    public void myDelete(String tableName, Key key) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            System.out.println("该表不存在!");
            return;
        }
        boolean removed = table.getTree().delete(key);
        System.out.println(removed ? "删除成功!" : "删除失败!");
    }

    /**
     * 更新数据
     */
    public void myUpdate(String tableName, Key key, List<Object> newRow) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            System.out.println("该表不存在!");
            return;
        }
        boolean updated = table.getTree().update(key, newRow);
        System.out.println(updated ? "更新成功!" : "更新失败!");
    }

    /**
     * 查询数据
     */
    public void myQuery(String tableName, String key, String condition) throws IOException {
        Table table = tables.get(tableName);
        if (table == null) {
            System.out.println("该表不存在!");
            return;
        }
        List<Object> row = null;
        // table.getTree().search(key);
        if (row == null) {
            System.out.println("未找到数据");
        } else {
            System.out.println("查询结果: " + row);
        }
    }

    /**
     * 获取表
     */
    public Table getTable(String tableName) {
        return tables.get(tableName); // O(1)
    }

    /**
     * 删除整个目录（递归）
     */
    private boolean deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        if (!deleteDirectory(file)) {
                            return false;
                        }
                    } else {
                        if (!file.delete()) {
                            return false;
                        }
                    }
                }
            }
        }
        return directory.delete();
    }
}
