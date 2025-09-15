package org.csu.mydb.storage.storageFiles.system;

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

//    //逻辑删除标记
//    private boolean isDeleted;

    public sysColumnsStructure(int columnId, int tableId, String columnName, String type, short length, short scale, short position, boolean isNullable, byte[] defaultValue) {
        this.columnId = columnId;
        this.tableId = tableId;
        this.columnName = columnName;
        this.type = type;
        this.length = length;
        this.scale = scale;
        this.position = position;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
//        this.isDeleted = isDeleted;
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

//    public boolean isDeleted() {
//        return isDeleted;
//    }
//
//    public void setDeleted(boolean deleted) {
//        isDeleted = deleted;
//    }
}
