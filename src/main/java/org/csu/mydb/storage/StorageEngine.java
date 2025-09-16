package org.csu.mydb.storage;

import org.csu.mydb.config.ConfigLoader;
import org.csu.mydb.storage.BPlusTree.BPlusTree;
import org.csu.mydb.storage.Table.Column.Column;
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
    private String prePath = "";       // 数据库路径前缀（如 "save/repos/"）
    private boolean isOpen = false;    // 是否已打开数据库
    private final List<Table> tables = new ArrayList<>();  // 当前打开的表列表

    public StorageEngine() {
        // 从 ConfigManager 获取配置
        StorageSystem storageSystem = new StorageSystem();
        storageSystem.getBufferPool().setPoolSize(ConfigLoader.getInstance().getInt("storage", "buffer_pool_size", 100));
        PageManager.PAGE_SIZE = ConfigLoader.getInstance().getInt("storage", "page_size", 4096);

        prePath = "";
        isOpen = false;
        tables.clear();

        System.out.println("存储引擎初始化: pageSize="
                + PageManager.PAGE_SIZE
                + ", bufferPoolSize=" + storageSystem.getBufferPool());
    }

    // 析构函数
    @Override
    protected void finalize() throws Throwable {
        try {
            myCloseDataBase();  // 关闭所有资源
        } finally {
            super.finalize();
        }
    }

    /**
     * 打开数据库
     *
     * @param dbName 数据库名
     */
    public void myOpenDataBase(String dbName) {

    }

    /**
     * 关闭数据库（对应 C++ 的 myCloseDataBase）
     */
    public void myCloseDataBase() {
        tables.clear();
        prePath = "";
        isOpen = false;
        System.out.println("关闭数据库成功");
    }

    /**
     * 创建数据库（对应 C++ 的 myCreateDataBase）
     * @param dataBaseName 数据库名
     */
    public void myCreateDataBase(String dataBaseName) {
        String path = "save/repos/" + dataBaseName;
        File dbDir = new File(path);
        if (dbDir.exists()) {
            System.out.println("该数据库已存在");
            return;
        }
        // 递归创建数据库目录（包括父目录）
        if (dbDir.mkdirs()) {
            System.out.println("创建数据库成功");
        } else {
            System.out.println("创建数据库失败（权限不足或路径无效）");
        }
    }

    /**
     * 删除数据库（对应 C++ 的 myDropDataBase）
     *
     * @param dataBaseName 数据库名
     */
    public void myDropDataBase(String dataBaseName) {
        String path = "save/repos/" + dataBaseName;
        File dbDir = new File(path);
        if (!dbDir.exists()) {
            System.out.println("该数据库不存在");
            return;
        }
        // 递归删除数据库目录下的所有文件和子目录
        if (deleteDirectory(dbDir)) {
            System.out.println("删除数据库成功");
        } else {
            System.out.println("删除数据库失败（存在无法删除的文件）");
        }
    }

    /**
     * 创建表（对应 C++ 的 myCreateTable）
     *
     * @param tableName 表名
     * @param columns   列名列表（如 ["id", "name"]）
     */
    public void myCreateTable(String tableName, List<Column> columns) {
    }

    /**
     * 删除表（对应 C++ 的 myDropTable）
     *
     * @param tableName 表名
     */
    public void myDropTable(String tableName) {
        if (!isOpen) {
            System.out.println("无选中数据库，请先打开数据库");
            return;
        }
        String tablePath = prePath + tableName + ".txt";
        File tableFile = new File(tablePath);
        // 从内存中移除表
        boolean removed = tables.removeIf(table -> table.getName().equals(tableName));
        if (!removed) {
            System.out.println("该表不存在!");
            return;
        }
        // 删除物理文件
        if (tableFile.delete()) {
            System.out.println("删除成功!");
        } else {
            System.out.println("删除失败（文件被占用或无权限）");
        }
    }

    /**
     * 检查表是否存在（对应 C++ 的 posIsNos）
     *
     * @param tableName 表名
     * @return 表在列表中的索引（-1 表示不存在）
     */
    public int posIsNos(String tableName) {
        for (int i = 0; i < tables.size(); i++) {
            if (tables.get(i).getName().equals(tableName)) {
                return i;  // 返回表索引
            }
        }
        return -1;  // 表不存在
    }

    /**
     * 插入数据（对应 C++ 的 myInsert）
     *
     * @param tableName 表名
     * @param columns   插入的列表
     * @param values   插入的值列表（如 ["1", "Alice"]）
     */
    public void myInsert(String tableName, List<Column> columns, List<String> values) {
    }

    /**
     * 删除数据（对应 C++ 的 myDelete）
     *
     * @param tableName 表名
     * @param condition 删除条件（如 "age = 18"）
     */
    public void myDelete(String tableName, String condition) {
    }

    /**
     * 更新数据（对应 C++ 的 myUpdate）
     *
     * @param tableName 表名
     * @param setCol    要更新的列名
     * @param newValue  新值
     * @param condition 更新条件（如 "age = 18"）
     */
    public void myUpdate(String tableName, String setCol, String newValue, String condition) {
    }

    /**
     * 查询数据
     *
     * @param tableName 表名
     * @param columns   要查询的列名（如 ["id", "name"]，"all" 表示所有列）
     * @param condition 查询条件（如 "age = 18"）
     */
    public void myQuery(String tableName, String columns, String condition) {
    }

    /**
     * 递归删除目录（工具方法，对应 C++ 的 remove 目录逻辑）
     * @param directory 要删除的目录
     * @return 是否成功删除
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

