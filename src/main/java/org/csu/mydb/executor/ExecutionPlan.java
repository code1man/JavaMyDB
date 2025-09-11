package org.csu.mydb.executor;



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
        QUERY
    }

    private OperationType operationType;
    private String databaseName;
    private String tableName;
    private List<String> columns;
    private List<String> values;
    private String condition;
    private String setColumn;
    private String newValue;
    private String queryColumns;

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

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
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

    @Override
    public String toString() {
        return "ExecutionPlan{" +
                "operationType=" + operationType +
                ", databaseName='" + databaseName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", columns=" + columns +
                ", values=" + values +
                ", condition='" + condition + '\'' +
                ", setColumn='" + setColumn + '\'' +
                ", newValue='" + newValue + '\'' +
                ", queryColumns='" + queryColumns + '\'' +
                '}';
    }
}