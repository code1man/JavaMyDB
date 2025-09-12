package org.csu.mydb.executor;

import org.csu.mydb.cli.ParsedCommand;
import java.util.Arrays;

/**
 * 将ParsedCommand转换为ExecutionPlan的转换器
 */
public class CommandToPlanConverter {

    /**
     * 将ParsedCommand转换为ExecutionPlan
     * @param command 解析后的命令
     * @return 执行计划
     */
    public static ExecutionPlan convert(ParsedCommand command) {
        // 将CommandType映射到OperationType
        ExecutionPlan.OperationType operationType = mapOperationType(command.getType());
        if (operationType == null) {
            return null;
        }

        ExecutionPlan plan = new ExecutionPlan(operationType);

        // 根据命令类型设置不同的参数
        switch (command.getType()) {
            case CREATE_DB:
            case DROP_DB:
            case OPEN_DB:
                plan.setDatabaseName(command.getTarget());
                break;

            case CREATE_TABLE:
            case DROP_TABLE:
                plan.setTableName(command.getTarget());
                if (command.getType() == ParsedCommand.CommandType.CREATE_TABLE && command.getParams() != null) {
                    plan.setColumns(Arrays.asList(command.getParams()));
                }
                break;

            case INSERT:
                plan.setTableName(command.getTarget());
                if (command.getParams() != null) {
                    plan.setValues(Arrays.asList(command.getParams()));
                }
                break;

            case UPDATE:
                plan.setTableName(command.getTarget());
                if (command.getParams() != null && command.getParams().length >= 3) {
                    plan.setSetColumn(command.getParams()[0]);
                    plan.setNewValue(command.getParams()[1]);
                    plan.setCondition(command.getParams()[2]);
                }
                break;

            case DELETE:
                plan.setTableName(command.getTarget());
                if (command.getParams() != null && command.getParams().length > 0) {
                    plan.setCondition(command.getParams()[0]);
                }
                break;

            case SELECT:
                plan.setTableName(command.getTarget());
                if (command.getParams() != null && command.getParams().length > 0) {
                    // 第一个参数是查询的列
                    plan.setQueryColumns(command.getParams()[0]);

                    // 如果有更多参数，则认为是条件
                    if (command.getParams().length > 1) {
                        plan.setCondition(command.getParams()[1]);
                    }
                }
                break;

            case CLOSE_DB:
            case EXIT:
                // 这些命令不需要额外参数
                break;
        }

        return plan;
    }

    /**
     * 将CommandType映射到OperationType
     * @param commandType CLI命令类型
     * @return 执行计划操作类型
     */
    private static ExecutionPlan.OperationType mapOperationType(ParsedCommand.CommandType commandType) {
        switch (commandType) {
            case CREATE_DB: return ExecutionPlan.OperationType.CREATE_DATABASE;
            case DROP_DB: return ExecutionPlan.OperationType.DROP_DATABASE;
            case OPEN_DB: return ExecutionPlan.OperationType.OPEN_DATABASE;
            case CLOSE_DB: return ExecutionPlan.OperationType.CLOSE_DATABASE;
            case CREATE_TABLE: return ExecutionPlan.OperationType.CREATE_TABLE;
            case DROP_TABLE: return ExecutionPlan.OperationType.DROP_TABLE;
            case INSERT: return ExecutionPlan.OperationType.INSERT;
            case UPDATE: return ExecutionPlan.OperationType.UPDATE;
            case DELETE: return ExecutionPlan.OperationType.DELETE;
            case SELECT: return ExecutionPlan.OperationType.QUERY;
            case EXIT: return ExecutionPlan.OperationType.EXIT;   // 新增 EXIT 映射
            default: return null;
        }
    }
}
