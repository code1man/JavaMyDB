package org.csu.mydb.storage;

//数据库表格中的列
public class Column {
    private String name;
    private String type;

    //数据长度
    private int length;

    //小数位数
    private int scale;

    //在表中是第几列
    private int position;

    //是不是主键
    private boolean isPrimaryKey;

    //能否为空
    private boolean isNullable;

    //默认值
    private byte[] defaultValue;

    public Column(String name, String type, int length, int scale, boolean isPrimaryKey, boolean isNullable, byte[] defaultValue) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.scale = scale;
        this.isPrimaryKey = isPrimaryKey;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setPrimaryKey(boolean primaryKey) {
        isPrimaryKey = primaryKey;
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

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }
}
