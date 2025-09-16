package org.csu.mydb.executor;



import org.csu.mydb.cli.ParsedCommand;
import org.csu.mydb.storage.StorageEngine;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;


/**
 * 执行器主类
 * 负责解析执行计划并调用存储引擎方法
 */
public class Executor {
    private StorageEngine storageEngine;

    public Executor() {
        this.storageEngine = new StorageEngine();
    }

    public Executor(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    /**
     * 执行给定的执行计划
     * @param plan 执行计划
     * @return 执行结果
     * @throws ExecutorException 执行异常
     */
    public ExecutionResult execute(ExecutionPlan plan) throws ExecutorException {
        try {
            switch (plan.getOperationType()) {
                case CREATE_DATABASE:
                    return executeCreateDatabase(plan);
                case DROP_DATABASE:
                    return executeDropDatabase(plan);
                case OPEN_DATABASE:
                    return executeOpenDatabase(plan);
                case CLOSE_DATABASE:
                    return executeCloseDatabase(plan);
                case CREATE_TABLE:
                    return executeCreateTable(plan);
                case DROP_TABLE:
                    return executeDropTable(plan);
                case INSERT:
                    return executeInsert(plan);
                case DELETE:
                    return executeDelete(plan);
                case UPDATE:
                    return executeUpdate(plan);
                case QUERY:
                    return executeQuery(plan);
                case EXIT:
                    return new ExecutionResult(true, "退出命令已执行");
                default:
                    throw new ExecutorException("未知的操作类型: " + plan.getOperationType());
            }
        } catch (Exception e) {
            throw new ExecutorException("执行计划失败: " + e.getMessage(), e);
        }
    }

    private ExecutionResult executeCreateDatabase(ExecutionPlan plan) {
        if (plan.getDatabaseName() == null || plan.getDatabaseName().isEmpty()) {
            return new ExecutionResult(false, "数据库名不能为空");
        }

        storageEngine.myCreateDataBase(plan.getDatabaseName());
        return new ExecutionResult(true, "数据库创建成功: " + plan.getDatabaseName());
    }

    private ExecutionResult executeDropDatabase(ExecutionPlan plan) {
        if (plan.getDatabaseName() == null || plan.getDatabaseName().isEmpty()) {
            return new ExecutionResult(false, "数据库名不能为空");
        }

        storageEngine.myDropDataBase(plan.getDatabaseName());
        return new ExecutionResult(true, "数据库删除成功: " + plan.getDatabaseName());
    }

    private ExecutionResult executeOpenDatabase(ExecutionPlan plan) {
        if (plan.getDatabaseName() == null || plan.getDatabaseName().isEmpty()) {
            return new ExecutionResult(false, "数据库名不能为空");
        }

        storageEngine.myOpenDataBase(plan.getDatabaseName());
        return new ExecutionResult(true, "数据库打开成功: " + plan.getDatabaseName());
    }

    private ExecutionResult executeCloseDatabase(ExecutionPlan plan) {
        storageEngine.myCloseDataBase();
        return new ExecutionResult(true, "数据库关闭成功");
    }
//传递列属性的增加9/16
    private ExecutionResult executeCreateTable(ExecutionPlan plan) {
        if (plan.getTableName() == null || plan.getTableName().isEmpty()) {
            return new ExecutionResult(false, "表名不能为空");
        }

        if (plan.getColumns() == null || plan.getColumns().isEmpty()) {
            return new ExecutionResult(false, "列定义不能为空");
        }

        storageEngine.myCreateTable(plan.getTableName(), plan.getColumns());
        return new ExecutionResult(true, "表创建成功: " + plan.getTableName());
    }

    private ExecutionResult executeDropTable(ExecutionPlan plan) {
        if (plan.getTableName() == null || plan.getTableName().isEmpty()) {
            return new ExecutionResult(false, "表名不能为空");
        }

        storageEngine.myDropTable(plan.getTableName());
        return new ExecutionResult(true, "表删除成功: " + plan.getTableName());
    }

    private ExecutionResult executeInsert(ExecutionPlan plan) {
        if (plan.getTableName() == null || plan.getTableName().isEmpty()) {
            return new ExecutionResult(false, "表名不能为空");
        }

        if (plan.getInsertColumns() == null || plan.getInsertColumns().isEmpty()) {
            return new ExecutionResult(false, "插入列不能为空");
        }

        storageEngine.myInsert(plan.getTableName(), plan.getInsertColumns());
        return new ExecutionResult(true, "数据插入成功", 1);
    }


    private ExecutionResult executeDelete(ExecutionPlan plan) {
        if (plan.getTableName() == null || plan.getTableName().isEmpty()) {
            return new ExecutionResult(false, "表名不能为空");
        }

        if (plan.getCondition() == null || plan.getCondition().isEmpty()) {
            return new ExecutionResult(false, "删除条件不能为空");
        }

        storageEngine.myDelete(plan.getTableName(), plan.getCondition());
        return new ExecutionResult(true, "数据删除成功");
    }

    private ExecutionResult executeUpdate(ExecutionPlan plan) {
        if (plan.getTableName() == null || plan.getTableName().isEmpty()) {
            return new ExecutionResult(false, "表名不能为空");
        }

        if (plan.getSetColumn() == null || plan.getSetColumn().isEmpty()) {
            return new ExecutionResult(false, "更新列不能为空");
        }

        if (plan.getNewValue() == null) {
            return new ExecutionResult(false, "新值不能为空");
        }

        if (plan.getCondition() == null || plan.getCondition().isEmpty()) {
            return new ExecutionResult(false, "更新条件不能为空");
        }

        storageEngine.myUpdate(plan.getTableName(), plan.getSetColumn(),
                plan.getNewValue(), plan.getCondition());
        return new ExecutionResult(true, "数据更新成功");
    }

    private ExecutionResult executeQuery(ExecutionPlan plan) {
        if (plan.getTableName() == null || plan.getTableName().isEmpty()) {
            return new ExecutionResult(false, "表名不能为空");
        }

        String columns = plan.getQueryColumns();
        if (columns == null || columns.isEmpty()) {
            columns = "all";
        }

        String condition = plan.getCondition();
        if (condition == null) {
            condition = "";
        }

        // 由于StorageEngine的myQuery方法直接输出到控制台，
        // 我们需要重定向输出以捕获结果
        // 这里我们创建一个自定义的输出捕获器
        QueryOutputCapturer capturer = new QueryOutputCapturer();
        capturer.startCapture();

        storageEngine.myQuery(plan.getTableName(), columns, condition);

        List<String> result = capturer.stopCapture();
        return new ExecutionResult(true, "查询成功", result);
    }

    /**
     * 内部类用于捕获查询输出
     */
    private static class QueryOutputCapturer {
        private java.io.ByteArrayOutputStream baos;
        private java.io.PrintStream originalOut;
        private java.io.PrintStream customOut;

        void startCapture() {
            baos = new java.io.ByteArrayOutputStream();
            customOut = new java.io.PrintStream(baos);
            originalOut = System.out;
            System.setOut(customOut);
        }

        List<String> stopCapture() {
            System.setOut(originalOut);
            customOut.close();

            String output = baos.toString();
            String[] lines = output.split("\n");

            List<String> result = new ArrayList<>();
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    result.add(line);
                }
            }

            return result;
        }
    }

    // 获取存储引擎实例（用于测试或其他用途）
    public StorageEngine getStorageEngine() {
        return storageEngine;
    }


    /**
     * 执行给定的ParsedCommand
     * @param command 解析后的命令
     * @return 是否继续执行CLI循环（true继续，false退出）
     * @throws ExecutorException 执行异常
     */
    public boolean execute(ParsedCommand command) throws ExecutorException {
        // 将ParsedCommand转换为ExecutionPlan
        ExecutionPlan plan = CommandToPlanConverter.convert(command);
        if (plan == null) {
            throw new ExecutorException("无法转换命令类型: " + command.getType());
        }

        // 执行转换后的计划
        ExecutionResult result = execute(plan);

        switch (plan.getOperationType()) {
            case EXIT:
                return false; // 明确退出 CLI
            case CLOSE_DATABASE:
                // 仅关闭数据库，不退出 CLI
                return true;
            default:
                return true;
        }
    }
}