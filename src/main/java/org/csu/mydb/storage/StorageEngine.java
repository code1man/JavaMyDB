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
    private final HashMap<String, Table> tableMap = new HashMap<>();
    private final StorageSystem storageSystem = new StorageSystem();

    public StorageEngine() {
        // 从 ConfigManager 获取配置
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
        String path = "save/repos/" + dbName;
        File dbDir = new File(path);

        // 检查数据库是否存在
        if (!dbDir.exists() || !dbDir.isDirectory()) {
            System.out.println("错误：数据库 '" + dbName + "' 不存在");
            return;
        }

        // 检查系统表是否存在
        File sysTablesFile = new File(dbDir, "sys_tables.dat");
        File sysColumnsFile = new File(dbDir, "sys_columns.dat");
        if (!sysTablesFile.exists() || !sysColumnsFile.exists()) {
            System.out.println("错误：数据库元数据损坏（缺失系统表）");
            return;
        }

        prePath = path;
        // storageSystem.loadAllTables(dbName, 3, 3).forEach(tables::add);
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
        int spaceId = storageSystem.createTable(prePath + tableName, columns);
        try {
            BPlusTree tree = new BPlusTree(3, spaceId, storageSystem, columns);
            Table table = new Table(tableName, prePath + tableName, columns, spaceId, tree);
            tables.add(table);
            tableMap.put(tableName, table);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        tableMap.remove(tableName);
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
        Table table = tableMap.get(tableName);
        List<Object> valuesList = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            if (Objects.equals(columns.get(i).getType(), "INT")) {
                valuesList.add(Integer.parseInt(values.get(i)));
            } else if (Objects.equals(columns.get(i).getType(), "VARCHAR")) {
                valuesList.add(values.get(i));
            } else {
                System.out.println("格式错误");
                return;
            }
        }
        try {
            table.getPrimaryIndex().insert(columns, valuesList);
            System.out.println("插入成功");
        } catch (IOException e) {
            System.out.println("插入失败");
        }
    }

    /**
     * 删除数据（对应 C++ 的 myDelete）
     *
     * @param tableName 表名
     * @param condition 删除条件（如 "age = 18"）
     */
    public void myDelete(String tableName, String condition) {
        List<Column> columns = tableMap.get(tableName).getColumns();
        String condCol = null;
        String condVal = null;
        if (condition != null && condition.contains("=")) {
            String[] parts = condition.split("=");
            condCol = parts[0].trim();
            condVal = parts[1].trim();
        }
        if (condCol == null) System.out.println("删除失败");;

        int pkIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(condCol) && columns.get(i).isPrimaryKey()) {
                pkIndex = i;
                break;
            }
        }
        if (pkIndex == -1)
            System.out.println("删除失败");

        Key key = new Key(Arrays.asList(parseValue(columns.get(pkIndex), condVal)), columns);
        try {
            tableMap.get(tableName).getTree().delete(key);
        } catch (IOException e) {
            System.out.println("删除失败");
        }
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
        List<Column> columns = tableMap.get(tableName).getColumns();
        // 解析条件
        String condCol = null;
        String condVal = null;
        if (condition != null && condition.contains("=")) {
            String[] parts = condition.split("=");
            condCol = parts[0].trim();
            condVal = parts[1].trim();
        }

        if (condCol == null)
            System.out.println("更新失败");;

        // 找主键索引
        int pkIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(condCol) && columns.get(i).isPrimaryKey()) {
                pkIndex = i;
                break;
            }
        }
        if (pkIndex == -1)
            System.out.println("更新失败");

        Key key = new Key(Arrays.asList(parseValue(columns.get(pkIndex), condVal)), columns);
        List<Object> row = null;
        try {
            row = tableMap.get(tableName).getTree().search(key);
        } catch (IOException e) {
            System.out.println("更新失败");
        }
        if (row == null)
            System.out.println("更新失败");

        // 更新列
        int updateIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equalsIgnoreCase(setCol)) {
                updateIndex = i;
                break;
            }
        }
        if (updateIndex == -1)
            System.out.println("更新失败");

        row.set(updateIndex, parseValue(columns.get(updateIndex), newValue));
    }

    /**
     * 查询数据
     *
     * @param tableName 表名
     * @param columns   要查询的列名（如 ["id", "name"]，"all" 表示所有列）
     * @param condition 查询条件（如 "age = 18"）
     */
    public void myQuery(String tableName, String columns, String condition) {
        List<Column> cols = tableMap.get(tableName).getColumns();
        List<List<Object>> results = new ArrayList<>();

        // 简单解析条件 "col = value"
        String condCol = null;
        String condValue = null;
        if (condition != null && !condition.isEmpty() && condition.contains("=")) {
            String[] parts = condition.split("=");
            condCol = parts[0].trim();
            condValue = parts[1].trim();
        }

        if (condCol != null) {
            // 只支持主键查询
            int pkIndex = -1;
            for (int i = 0; i < cols.size(); i++) {
                if (cols.get(i).getName().equalsIgnoreCase(condCol) && cols.get(i).isPrimaryKey()) {
                    pkIndex = i;
                    break;
                }
            }
            if (pkIndex != -1) {
                Key key = new Key(Arrays.asList(parseValue(cols.get(pkIndex), condValue)), cols);
                List<Object> row = null;
                try {
                    row = tableMap.get(tableName).getTree().search(key);
                } catch (IOException e) {
                    System.out.println("更新失败");
                }
                if (row != null) results.add(row);
            }
        } else {
            // 条件为空，返回全部（此处简化，实际需遍历叶子节点链表）
            // 暂时不实现完整扫描
        }
        System.out.println(results.toString());
    }



    // 带 JOIN 的多表查询,重构方法,和MyQuery是一样的,只是参数类型不一样
    public void myQuery(String tableName, String joinTableName,
                        String columns, String joinCondition, String condition) {
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

    // ===================== 工具 =====================
    private Object parseValue(Column col, String val) {
        switch (col.getType()) {
            case "INT": return Integer.parseInt(val);
            case "VARCHAR": return val;
            default: return val;
        }
    }
}

