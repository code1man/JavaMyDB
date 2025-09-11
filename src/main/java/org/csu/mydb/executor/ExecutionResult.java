package org.csu.mydb.executor;



import java.util.List;

/**
 * 执行结果封装类
 */
public class ExecutionResult {
    private boolean success;
    private String message;
    private List<String> data;
    private int affectedRows;

    public ExecutionResult(boolean success, String message) {
        this.success = success;
        this.message = message;
        this.affectedRows = 0;
    }

    public ExecutionResult(boolean success, String message, int affectedRows) {
        this.success = success;
        this.message = message;
        this.affectedRows = affectedRows;
    }

    public ExecutionResult(boolean success, String message, List<String> data) {
        this.success = success;
        this.message = message;
        this.data = data;
    }

    // Getter和Setter方法
    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<String> getData() {
        return data;
    }

    public void setData(List<String> data) {
        this.data = data;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(int affectedRows) {
        this.affectedRows = affectedRows;
    }

    @Override
    public String toString() {
        if (data != null && !data.isEmpty()) {
            return String.join("\n", data);
        }
        return message;
    }
}