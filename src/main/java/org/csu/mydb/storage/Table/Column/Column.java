package org.csu.mydb.storage.Table.Column;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    public Column(String name, String type, int length, int scale, int position, boolean isPrimaryKey, boolean isNullable, byte[] defaultValue) {
        this.name = name;
        this.type = type;
        this.length = length;
        this.scale = scale;
        this.isPrimaryKey = isPrimaryKey;
        this.isNullable = isNullable;
        this.defaultValue = defaultValue;
        this.position = position;
    }

    public Column() {
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

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public static Column parseColumn(String columnDefinition, int position) {
        // 初始化默认值（可根据需求调整）
        String name = null;
        String type = null;
        int length = -1;       // 默认-1表示无长度限制（如INT）
        int scale = -1;        // 默认-1表示无小数位（如INT）
        boolean isPrimaryKey = false;
        boolean isNullable = true;  // 默认允许NULL
        byte[] defaultValue = null;

        // 步骤1：提取列名（必须以字母/下划线开头）
        Pattern namePattern = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*");
        Matcher nameMatcher = namePattern.matcher(columnDefinition);
        if (!nameMatcher.find()) {
            throw new IllegalArgumentException("列名格式错误（需以字母/下划线开头）: " + columnDefinition);
        }
        name = nameMatcher.group();
        String remaining = columnDefinition.substring(nameMatcher.end()).trim();  // 剩余部分

        // 步骤2：提取数据类型（含长度/小数位参数）
        Pattern typePattern = Pattern.compile("^([A-Za-z]+)(\\(([^)]+)\\))?");
        Matcher typeMatcher = typePattern.matcher(remaining);
        if (!typeMatcher.find()) {
            throw new IllegalArgumentException("数据类型格式错误: " + remaining);
        }
        type = typeMatcher.group(1).toUpperCase();  // 类型名统一大写（如"varchar"→"VARCHAR"）
        String params = typeMatcher.group(3);       // 参数部分（如"10"或"5,2"）

        // 解析参数（长度、小数位）
        if (params != null) {
            String[] paramParts = params.split(",");
            if (paramParts.length > 0) {
                try {
                    length = Integer.parseInt(paramParts[0].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("数据类型长度参数格式错误: " + paramParts[0], e);
                }
            }
            if (paramParts.length > 1) {
                try {
                    scale = Integer.parseInt(paramParts[1].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("数据类型小数位参数格式错误: " + paramParts[1], e);
                }
            }
        }
        remaining = remaining.substring(typeMatcher.end()).trim().toUpperCase();  // 剩余约束部分

        // 步骤3：解析约束条件（主键、非空、默认值等）
        while (!remaining.isEmpty()) {
            // 匹配 PRIMARY KEY（主键约束）
            if (remaining.startsWith("PRIMARY KEY")) {
                isPrimaryKey = true;
                remaining = remaining.substring("PRIMARY KEY".length()).trim();
            }
            // 匹配 NOT NULL（非空约束）
            else if (remaining.startsWith("NOT NULL")) {
                isNullable = false;
                remaining = remaining.substring("NOT NULL".length()).trim();
            }
            // 匹配 DEFAULT（默认值约束）
            else if (remaining.startsWith("DEFAULT")) {
                remaining = remaining.substring("DEFAULT".length()).trim();
                if (remaining.isEmpty()) {
                    throw new IllegalArgumentException("DEFAULT约束缺少值: " + columnDefinition);
                }
                // 处理字符串默认值（带单引号）
                if (remaining.startsWith("'") && remaining.endsWith("'")) {
                    String valueStr = remaining.substring(1, remaining.length() - 1);
                    defaultValue = valueStr.getBytes(StandardCharsets.UTF_8);
                    remaining = "";
                }
                // 处理数字默认值（整数/浮点数）
                else if (remaining.matches("^-?\\d+(\\.\\d+)?$")) {  // 简单数字匹配（可扩展）
                    defaultValue = remaining.getBytes(StandardCharsets.UTF_8);
                    remaining = "";
                }
                // 其他类型（如布尔值）可在此扩展
                else {
                    throw new IllegalArgumentException("不支持的DEFAULT值格式: " + remaining);
                }
            }
            // 未知约束（可扩展）
            else {
                throw new IllegalArgumentException("不支持的约束条件: " + remaining);
            }
        }

        // 创建并填充Column对象
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        column.setLength(length);
        column.setScale(scale);
        column.setPosition(position);
        column.setPrimaryKey(isPrimaryKey);
        column.setNullable(isNullable);
        column.setDefaultValue(defaultValue);
        return column;
    }
}
