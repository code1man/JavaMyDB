package org.csu.mydb.storage;

//数据库表格中的列
public class Column {
    private String name;
    private String type;
    private boolean isPrimaryKey;
    private boolean isNullable;
    private byte[] defaultValue;

    public Column(String name, String type, boolean isPrimaryKey, boolean isNullable, byte[] defaultValue) {
        this.name = name;
        this.type = type;
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
}
