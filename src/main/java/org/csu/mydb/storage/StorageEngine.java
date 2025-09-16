package org.csu.mydb.storage;

import org.csu.mydb.config.ConfigLoader;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Table;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

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
//        File dbDir = new File("save/repos/" + dbName);
//        if (!dbDir.exists()) {
//            System.out.println("数据库不存在");
//            return;
//        }
//        prePath = dbDir.getPath() + "/";
//        isOpen = true;
//        // 加载表元信息
//        for (File file : dbDir.listFiles()) {
//            if (file.getName().endsWith(".ibd")) {
//                String tableName = file.getName().replace(".ibd", "");
//                int spaceId = SpaceManager.allocateSpace();
//                BPlusTree<Integer> primaryIndex = new BPlusTree<>();
//                tables.add(new Table(tableName, file.getPath(), new ArrayList<>(), spaceId, primaryIndex));
//            }
//        }
//        System.out.println("打开数据库成功: " + dbName);
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
//        if (!isOpen) {
//            System.out.println("无选中数据库，请先打开数据库");
//            return;
//        }
//        String tablePath = prePath + tableName + ".txt";
//        File tableFile = new File(tablePath);
//        if (tableFile.exists()) {
//            System.out.println("该表已经存在!");
//            return;
//        }
//        try (FileWriter writer = new FileWriter(tableFile)) {
//            writer.write("表名:" + tableName + "\n");
//            for (String col : columns) {
//                writer.write(col + "\n");
//            }
//            writer.write("\n"); // 空行分隔元数据和数据
//            tables.add(new Table(tableName, tablePath, columns));
//            System.out.println("创建新表成功!");
//        } catch (IOException e) {
//            System.out.println("创建表失败: " + e.getMessage());
//        }
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
     * @param insertColumns   插入的值列表（如 ["1", "Alice"]）
     */
    public void myInsert(String tableName, List<Column> insertColumns, List<String> values) {
//        if (!isOpen) {
//            System.out.println("无选中数据库!");
//            return;
//        }
//        int tableIndex = posIsNos(tableName);
//        if (tableIndex == -1) {
//            System.out.println("该表不存在!");
//            return;
//        }
//        Table table = tables.get(tableIndex);
//        String tablePath = table.getPath();
//
//        // 检查值的数量是否与列数匹配
//        if (values.size() != table.getColumns().size()) {
//            System.out.println("插入失败：列数与值数量不匹配");
//            return;
//        }
//
//        // 追加写入数据到表文件（格式：值1 值2 ...）
//        try (FileWriter writer = new FileWriter(tablePath, true)) {  // true 表示追加模式
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i < values.size(); i++) {
//                sb.append(values.get(i));
//                if (i != values.size() - 1) {
//                    sb.append(" ");  // 用空格分隔值（与 C++ 逻辑一致）
//                }
//            }
//            sb.append("\n");  // 换行分隔记录
//            writer.write(sb.toString());
//            System.out.println("插入成功!");
//        } catch (IOException e) {
//            System.out.println("插入失败: " + e.getMessage());
//        }
    }

    /**
     * 删除数据（对应 C++ 的 myDelete）
     *
     * @param tableName 表名
     * @param condition 删除条件（如 "age = 18"）
     */
    public void myDelete(String tableName, String condition) {
//        if (!isOpen) {
//            System.out.println("无选中数据库!");
//            return;
//        }
//        int tableIndex = posIsNos(tableName);
//        if (tableIndex == -1) {
//            System.out.println("该表不存在!");
//            return;
//        }
//        Table table = tables.get(tableIndex);
//        String tablePath = table.getPath();
//        String tempPath = "save/repos/tmp.txt";  // 临时文件路径
//
//        try (BufferedReader reader = new BufferedReader(new FileReader(tablePath));
//             BufferedWriter writer = new BufferedWriter(new FileWriter(tempPath))) {
//
//            String line;
//            String[] conditionParts = condition.split("=");
//            if (conditionParts.length != 2) {
//                System.out.println("删除条件格式错误（示例：age = 18）");
//                return;
//            }
//            String targetCol = conditionParts[0].trim();
//            String targetValue = conditionParts[1].trim();
//
//            // 查找目标列的索引
//            int targetColIndex = -1;
//            List<String> columns = table.getColumns();
//            for (int i = 0; i < columns.size(); i++) {
//                if (columns.get(i).equals(targetCol)) {
//                    targetColIndex = i;
//                    break;
//                }
//            }
//            if (targetColIndex == -1) {
//                System.out.println("删除失败：列名不存在");
//                return;
//            }
//
//            // 复制符合条件的行到临时文件
//            boolean isHeader = true;  // 标记是否为表头
//            while ((line = reader.readLine()) != null) {
//                if (isHeader) {
//                    writer.write(line + "\n");  // 保留表头
//                    isHeader = false;
//                    continue;
//                }
//                String[] values = line.trim().split(" ");
//                if (values.length != columns.size()) {
//                    continue;  // 跳过格式错误的行
//                }
//                if (values[targetColIndex].equals(targetValue)) {
//                    continue;  // 跳过符合条件的行（不复制）
//                }
//                writer.write(line + "\n");  // 复制不符合条件的行
//            }
//            // 删除原文件并重命名临时文件
//            reader.close();
//            writer.close();
//            if (new File(tablePath).delete() && new File(tempPath).renameTo(new File(tablePath))) {
//                System.out.println("删除成功!");
//            } else {
//                System.out.println("删除失败（文件操作失败）");
//                new File(tempPath).delete();  // 清理临时文件
//            }
//        } catch (IOException e) {
//            System.out.println("删除失败: " + e.getMessage());
//            try {
//                new File(tempPath).delete();  // 清理临时文件
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }
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
//        if (!isOpen) {
//            System.out.println("无选中数据库!");
//            return;
//        }
//        int tableIndex = posIsNos(tableName);
//        if (tableIndex == -1) {
//            System.out.println("该表不存在!");
//            return;
//        }
//        Table table = tables.get(tableIndex);
//        String tablePath = table.getPath();
//        String tempPath = "save/repos/tmp.txt";  // 临时文件路径
//
//        try (BufferedReader reader = new BufferedReader(new FileReader(tablePath));
//             BufferedWriter writer = new BufferedWriter(new FileWriter(tempPath))) {
//
//            String line;
//            String[] conditionParts = condition.split("=");
//            if (conditionParts.length != 2) {
//                System.out.println("更新条件格式错误（示例：age = 18）");
//                return;
//            }
//            String targetCol = conditionParts[0].trim();
//            String targetValue = conditionParts[1].trim();
//
//            // 查找目标列和更新列的索引
//            int targetColIndex = -1;
//            int setColIndex = -1;
//            List<String> columns = table.getColumns();
//            for (int i = 0; i < columns.size(); i++) {
//                if (columns.get(i).equals(targetCol)) {
//                    targetColIndex = i;
//                }
//                if (columns.get(i).equals(setCol)) {
//                    setColIndex = i;
//                }
//            }
//            if (targetColIndex == -1 || setColIndex == -1) {
//                System.out.println("更新失败：列名不存在");
//                return;
//            }
//
//            // 复制并更新符合条件的行到临时文件
//            boolean isHeader = true;  // 标记是否为表头
//            while ((line = reader.readLine()) != null) {
//                if (isHeader) {
//                    writer.write(line + "\n");  // 保留表头
//                    isHeader = false;
//                    continue;
//                }
//                String[] values = line.trim().split(" ");
//                if (values.length != columns.size()) {
//                    continue;  // 跳过格式错误的行
//                }
//                if (values[targetColIndex].equals(targetValue)) {
//                    values[setColIndex] = newValue;  // 更新值
//                }
//                // 写入更新后的行
//                writer.write(String.join(" ", values) + "\n");
//            }
//            // 删除原文件并重命名临时文件
//            reader.close();
//            writer.close();
//            if (new File(tablePath).delete() && new File(tempPath).renameTo(new File(tablePath))) {
//                System.out.println("更新成功!");
//            } else {
//                System.out.println("更新失败（文件操作失败）");
//                new File(tempPath).delete();  // 清理临时文件
//            }
//        } catch (IOException e) {
//            System.out.println("更新失败: " + e.getMessage());
//            try {
//                new File(tempPath).delete();  // 清理临时文件
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        }
    }
    //权限管理
    public void  myGrant(String databaseName, String grantee, List<String> grants){
        System.out.println(databaseName + grantee + grants);
    }
    /**
     * 查询数据
     *
     * @param tableName 表名
     * @param columns   要查询的列名（如 ["id", "name"]，"all" 表示所有列）
     * @param condition 查询条件（如 "age = 18"）
     */
    public void myQuery(String tableName, String columns, String condition) {
//        if (!isOpen) {
//            System.out.println("无选中数据库!");
//            return;
//        }
//        int tableIndex = posIsNos(tableName);
//        if (tableIndex == -1) {
//            System.out.println("该表不存在!");
//            return;
//        }
//        Table table = tables.get(tableIndex);
//        String tablePath = table.getPath();
//
//        // 处理查询列（"all" 表示所有列）
//        List<String> queryCols;
//        if (columns.equalsIgnoreCase("all")) {
//            queryCols = table.getColumns();
//        } else {
//            queryCols = new ArrayList<>();
//            for (String col : columns.split(",")) {
//                queryCols.add(col.trim());
//            }
//        }
//
//        // 查找查询列的索引
//        List<Integer> colIndices = new ArrayList<>();
//        List<String> tableColumns = table.getColumns();
//        for (String col : queryCols) {
//            int idx = -1;
//            for (int i = 0; i < tableColumns.size(); i++) {
//                if (tableColumns.get(i).equals(col)) {
//                    idx = i;
//                    break;
//                }
//            }
//            if (idx == -1) {
//                System.out.println("查询失败：列名 " + col + " 不存在");
//                return;
//            }
//            colIndices.add(idx);
//        }
//
//        // 读取并过滤数据
//        try (BufferedReader reader = new BufferedReader(new FileReader(tablePath))) {
//            String line;
//            boolean isHeader = true;  // 标记是否为表头
//            while ((line = reader.readLine()) != null) {
//                if (isHeader) {
//                    // 打印表头（仅查询的列）
//                    StringBuilder header = new StringBuilder();
//                    for (int idx : colIndices) {
//                        header.append(tableColumns.get(idx)).append(" ");
//                    }
//                    System.out.println(header.toString().trim());
//                    isHeader = false;
//                    continue;
//                }
//                String[] values = line.trim().split(" ");
//                if (values.length != tableColumns.size()) {
//                    continue;  // 跳过格式错误的行
//                }
//                // 应用查询条件（示例：仅支持 "col = value" 格式）
//                boolean conditionMet = true;
//                if (!condition.isEmpty()) {
//                    String[] condParts = condition.split("=");
//                    if (condParts.length != 2) {
//                        System.out.println("查询条件格式错误（示例：age = 18）");
//                        return;
//                    }
//                    String condCol = condParts[0].trim();
//                    String condValue = condParts[1].trim();
//                    int condColIndex = -1;
//                    for (int i = 0; i < tableColumns.size(); i++) {
//                        if (tableColumns.get(i).equals(condCol)) {
//                            condColIndex = i;
//                            break;
//                        }
//                    }
//                    if (condColIndex == -1 || !values[condColIndex].equals(condValue)) {
//                        conditionMet = false;
//                    }
//                }
//                if (conditionMet) {
//                    // 打印符合条件的行（仅查询的列）
//                    StringBuilder row = new StringBuilder();
//                    for (int idx : colIndices) {
//                        row.append(values[idx]).append(" ");
//                    }
//                    System.out.println(row.toString().trim());
//                }
//            }
//        } catch (IOException e) {
//            System.out.println("查询失败: " + e.getMessage());
//        }
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
}
