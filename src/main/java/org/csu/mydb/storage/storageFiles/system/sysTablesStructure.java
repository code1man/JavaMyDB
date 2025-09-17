package org.csu.mydb.storage.storageFiles.system;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

//系统表sys_tables结构的硬编码
public class sysTablesStructure {
    private int tableId;
    private String tableName;
    private int spaceId;
    private int rootPage;

    //b+树层数
    private int order;

    private String databaseName; // 新增字段

//    //逻辑删除标记
//    private boolean isDeleted;


    public sysTablesStructure(int tableId, String tableName, int spaceId, int rootPage, int order, String databaseName) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.spaceId = spaceId;
        this.rootPage = rootPage;
        this.order = order;
        this.databaseName = databaseName;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    public int getRootPage() {
        return rootPage;
    }

    public void setRootPage(int rootPage) {
        this.rootPage = rootPage;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

//    public boolean isDeleted() {
//        return isDeleted;
//    }
//
//    public void setDeleted(boolean deleted) {
//        isDeleted = deleted;
//    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public sysTablesStructure() {
    }

    // 序列化方法
    public byte[] toBytes() {
        // 计算总字节大小：
        // tableId (4) + tableName (64) + databaseName (64) + spaceId (4) + rootPage (4) + order (4)
        int size = 4 + 64 + 64 + 4 + 4 + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        // 写入字段
        buffer.putInt(tableId);                   // 4 bytes
        putString(buffer, tableName, 64);         // 64 bytes (固定长度)
        putString(buffer, databaseName, 64);      // 64 bytes (固定长度)
        buffer.putInt(spaceId);                   // 4 bytes
        buffer.putInt(rootPage);                  // 4 bytes
        buffer.putInt(order);                     // 4 bytes (新增)

        return buffer.array();
    }

    // 辅助方法：写入固定长度的字符串（不足补'\0'，过长截断）
    private void putString(ByteBuffer buffer, String str, int length) {
        byte[] strBytes = new byte[length];
        if (str != null) {
            byte[] srcBytes = str.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(srcBytes, 0, strBytes, 0, Math.min(srcBytes.length, length));
        }
        buffer.put(strBytes);
    }

    // 反序列化方法
    public static sysTablesStructure fromBytes(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        sysTablesStructure record = new sysTablesStructure();

        // 读取字段
        record.tableId = buffer.getInt();                   // 4 bytes
        record.tableName = getString(buffer, 64);           // 64 bytes
        record.databaseName = getString(buffer, 64);        // 64 bytes
        record.spaceId = buffer.getInt();                   // 4 bytes
        record.rootPage = buffer.getInt();                  // 4 bytes
        record.order = buffer.getInt();                     // 4 bytes (新增)

        return record;
    }

    // 辅助方法：读取固定长度的字符串（去除多余的'\0'）
    private static String getString(ByteBuffer buffer, int length) {
        byte[] strBytes = new byte[length];
        buffer.get(strBytes);
        int end = 0;
        while (end < length && strBytes[end] != 0) {
            end++;
        }
        return new String(strBytes, 0, end, StandardCharsets.UTF_8);
    }
}
