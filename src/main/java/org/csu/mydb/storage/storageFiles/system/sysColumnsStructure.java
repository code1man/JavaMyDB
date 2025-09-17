package org.csu.mydb.storage.storageFiles.system;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

//系统表sys_tables结构的硬编码
public class sysColumnsStructure {
    private int columnId;
    private int tableId;
    private String columnName;
    private String type;

    //数据长度，如varchar(50)
    private short length;

    //小数位数
    private short scale;

    //这一列在表格中属于第几列
    private short position;

    //是否可为空值
    private boolean isNullable;

    //默认值
    private byte[] defaultValue;

    //是否主键
    private boolean isPrimaryKey;
//    //逻辑删除标记
//    private boolean isDeleted;

    public sysColumnsStructure() {
    }

    public sysColumnsStructure(int columnId, int tableId, String columnName, String type, short length, short scale, short position, boolean isNullable, byte[] defaultValue, boolean isPrimaryKey) {
        this.columnId = columnId;
        this.tableId = tableId;
        this.columnName = columnName;
        this.type = type;
        this.length = length;
        this.scale = scale;
        this.position = position;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.isPrimaryKey = isPrimaryKey;
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public short getLength() {
        return length;
    }

    public void setLength(short length) {
        this.length = length;
    }

    public short getScale() {
        return scale;
    }

    public void setScale(short scale) {
        this.scale = scale;
    }

    public short getPosition() {
        return position;
    }

    public void setPosition(short position) {
        this.position = position;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean nullable) {
        isNullable = nullable;
    }

    public byte[] getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(byte[] defaultValue) {
        this.defaultValue = defaultValue;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
    }
//    public boolean isDeleted() {
//        return isDeleted;
//    }
//
//    public void setDeleted(boolean deleted) {
//        isDeleted = deleted;
//    }

    // ========== 序列化方法 ==========
    public byte[] toBytes() {
        // 计算总长度：
        // columnId(4) + tableId(4) + columnName(64) + type(64) +
        // length(2) + scale(2) + position(2) + isNullable(1) +
        // isPrimaryKey(1) + defaultValue(变长)
        int fixedLength = 4 + 4 + 64 + 64 + 2 + 2 + 2 + 1 + 1;
        int defaultValueLength = (defaultValue != null) ? defaultValue.length : 0;
        int totalLength = fixedLength + defaultValueLength;

        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // 写入固定长度字段
        buffer.putInt(columnId);
        buffer.putInt(tableId);
        putFixedString(buffer, columnName, 64);
        putFixedString(buffer, type, 64);
        buffer.putShort(length);
        buffer.putShort(scale);
        buffer.putShort(position);
        buffer.put((byte) (isNullable ? 1 : 0));
        buffer.put((byte) (isPrimaryKey ? 1 : 0));  // 新增主键标记

        // 写入变长默认值
        if (defaultValue != null) {
            buffer.put(defaultValue);
        }

        return buffer.array();
    }

    // 辅助方法：写入固定长度字符串（不足补'\0'）
    private void putFixedString(ByteBuffer buffer, String str, int length) {
        byte[] strBytes = new byte[length];
        if (str != null) {
            byte[] srcBytes = str.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(srcBytes, 0, strBytes, 0, Math.min(srcBytes.length, length));
        }
        buffer.put(strBytes);
    }

    // ========== 反序列化方法 ==========
    public static sysColumnsStructure fromBytes(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        sysColumnsStructure column = new sysColumnsStructure();

        // 读取固定长度字段
        column.columnId = buffer.getInt();
        column.tableId = buffer.getInt();
        column.columnName = getFixedString(buffer, 64);
        column.type = getFixedString(buffer, 64);
        column.length = buffer.getShort();
        column.scale = buffer.getShort();
        column.position = buffer.getShort();
        column.isNullable = (buffer.get() == 1);
        column.isPrimaryKey = (buffer.get() == 1);  // 新增主键标记读取

        // 读取变长默认值
        int remaining = buffer.remaining();
        if (remaining > 0) {
            byte[] defaultValue = new byte[remaining];
            buffer.get(defaultValue);
            column.defaultValue = defaultValue;
        }

        return column;
    }

    // 辅助方法：读取固定长度字符串（去除补零）
    private static String getFixedString(ByteBuffer buffer, int length) {
        byte[] strBytes = new byte[length];
        buffer.get(strBytes);
        int end = 0;
        while (end < length && strBytes[end] != 0) {
            end++;
        }
        return new String(strBytes, 0, end, StandardCharsets.UTF_8);
    }

}
