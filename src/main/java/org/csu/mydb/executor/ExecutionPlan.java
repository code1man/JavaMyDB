package org.csu.mydb.executor;



import org.csu.mydb.storage.Table.Column.Column;

import java.util.List;

/**
 * 执行计划数据结构
 * 包含要执行的操作类型和参数
 */
public class ExecutionPlan {

    public enum OperationType {
        CREATE_DATABASE,
        DROP_DATABASE,
        OPEN_DATABASE,
        CLOSE_DATABASE,
        CREATE_TABLE,
        DROP_TABLE,
        INSERT,
        DELETE,
        UPDATE,
        QUERY,
        EXIT
    }

    private OperationType operationType;
    private String databaseName;
    private String tableName;
    private List<Column> columns;
    private List<String> insertColumns;
    private String condition;
    private String setColumn;
    private String newValue;
    private String queryColumns;
    private String tableAlias;
    private String joinTableName;
    private String joinTableAlias;
    private String joinCondition;
    // 构造函数
    public ExecutionPlan(OperationType operationType) {
        this.operationType = operationType;
    }
    // Getter和Setter方法
    public OperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(OperationType operationType) {
        this.operationType = operationType;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    // 修改 getter 和 setter 方法
    public List<Column> getColumns() {
        return columns;
    }


    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }


    // Getter & Setter
    public List<String> getValues() {
        return insertColumns;
    }

    public void setValues(List<String> insertColumns) {
        this.insertColumns = insertColumns;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public String getSetColumn() {
        return setColumn;
    }

    public void setSetColumn(String setColumn) {
        this.setColumn = setColumn;
    }

    public String getNewValue() {
        return newValue;
    }

    public void setNewValue(String newValue) {
        this.newValue = newValue;
    }

    public String getQueryColumns() {
        return queryColumns;
    }

    public void setQueryColumns(String queryColumns) {
        this.queryColumns = queryColumns;
    }
    public String getTableAlias() { return tableAlias; }
    public void setTableAlias(String a) { this.tableAlias = a; }

    public String getJoinTableName() { return joinTableName; }
    public void setJoinTableName(String t) { this.joinTableName = t; }

    public String getJoinTableAlias() { return joinTableAlias; }
    public void setJoinTableAlias(String a) { this.joinTableAlias = a; }

    public String getJoinCondition() { return joinCondition; }
    public void setJoinCondition(String c) { this.joinCondition = c; }

    @Override
    public String toString() {
        return "ExecutionPlan{" +
                "operationType=" + operationType +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", values=" + insertColumns +
                ", condition='" + condition + '\'' +
                ", setColumn='" + setColumn + '\'' +
                ", newValue='" + newValue + '\'' +
                ", queryColumns='" + queryColumns + '\'' +
                ", tableAlias='" + tableAlias + '\'' +
                ", joinTableName='" + joinTableName + '\'' +
                ", joinTableAlias='" + joinTableAlias + '\'' +
                ", joinCondition='" + joinCondition + '\'' +
                '}';
    }
}