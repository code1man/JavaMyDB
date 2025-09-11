package org.csu.mydb.executor;

import org.csu.mydb.cli.ParsedCommand;
import org.csu.mydb.storage.StorageEngine;

import java.util.ArrayList;
import java.util.List;

public class Executor {
    private final StorageEngine storage_;  // 持有存储引擎的引用（Java 中用 final 强制不可变）

    /**
     * 构造函数（对应 C++ 的 Executor::Executor(StorageEngine& storage)）
     */
    public Executor(StorageEngine storage) {
        this.storage_ = storage;
    }

    /**
     * 执行命令（对应 C++ 的 Executor::execute）
     */
    public boolean execute(ParsedCommand cmd) {
        if (cmd == null || cmd.getType() == null) {
            return false;
        }
        return switch (cmd.getType()) {
            case CREATE_DB -> executeCreateDatabase(cmd);
            case DROP_DB -> executeDropDatabase(cmd);
            case OPEN_DB -> executeOpenDatabase(cmd);
            case CLOSE_DB -> executeCloseDatabase(cmd);
            case CREATE_TABLE -> executeCreateTable(cmd);
            case DROP_TABLE -> executeDropTable(cmd);
            case EXIT -> false;  // 退出命令返回 false，终止循环
            default -> {
                System.out.println("未知命令或未实现");
                yield true;
            }
        };
    }

    /**
     * 执行创建数据库（对应 C++ 的 execute_create_database）
     */
    private boolean executeCreateDatabase(ParsedCommand cmd) {
        if (cmd.getTarget().isEmpty()) {
            System.out.println("Database name missing.");
            return true;
        }
        storage_.myCreateDataBase(cmd.getTarget());
        return true;
    }

    /**
     * 执行删除数据库（对应 C++ 的 execute_drop_database）
     */
    private boolean executeDropDatabase(ParsedCommand cmd) {
        if (cmd.getTarget().isEmpty()) {
            System.out.println("Database name missing.");
            return true;
        }
        storage_.myDropDataBase(cmd.getTarget());
        return true;
    }

    /**
     * 执行打开数据库（对应 C++ 的 execute_open_database）
     */
    private boolean executeOpenDatabase(ParsedCommand cmd) {
        if (cmd.getTarget().isEmpty()) {
            System.out.println("Database name missing.");
            return true;
        }
        storage_.myOpenDataBase(cmd.getTarget());
        return true;
    }

    /**
     * 执行关闭数据库（对应 C++ 的 execute_close_database）
     */
    private boolean executeCloseDatabase(ParsedCommand cmd) {
        storage_.myCloseDataBase();
        return true;
    }

    /**
     * 执行创建表（对应 C++ 的 execute_create_table，完整列信息处理）
     */
    private boolean executeCreateTable(ParsedCommand cmd) {
        if (cmd.getTarget().isEmpty()) {
            System.out.println("错误：表名缺失（示例：create table 表名 (列1 类型, 列2 类型)）");
            return false;
        }

        String[] columnDefs = cmd.getParams();
        if (columnDefs == null || columnDefs.length == 0) {
            System.out.println("错误：列定义缺失（示例：create table 表名 (列1 类型, 列2 类型)）");
            return false;
        }

        // 校验列定义格式（允许类型带长度，如 VARCHAR(255)）
        List<String> validColumns = new ArrayList<>();
        for (String colDef : columnDefs) {
            // 正则匹配：列名（字母开头） + 空格 + 类型（字母开头，可选括号内长度）
            if (colDef.matches("^[a-zA-Z_][a-zA-Z0-9_]*\\s+[a-zA-Z]+(\\([^)]+\\))?$")) {
                validColumns.add(colDef);
            } else {
                System.out.println("警告：无效的列定义 '" + colDef + "'，已跳过");
            }
        }

        if (validColumns.isEmpty()) {
            System.out.println("错误：无有效列定义");
            return false;
        }

        // 调用存储引擎创建表
        try {
            storage_.myCreateTable(cmd.getTarget(), validColumns);
            System.out.println("表 '" + cmd.getTarget() + "' 创建成功，列定义：" + validColumns);
            return true;
        } catch (Exception e) {
            System.out.println("错误：创建表失败 - " + e.getMessage());
            return false;
        }
    }

    /**
     * 执行删除表（对应 C++ 的 execute_drop_table）
     */
    private boolean executeDropTable(ParsedCommand cmd) {
        if (cmd.getTarget().isEmpty()) {
            System.out.println("Table name missing.");
            return true;
        }
        storage_.myDropTable(cmd.getTarget());
        return true;
    }
}