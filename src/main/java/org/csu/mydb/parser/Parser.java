package org.csu.mydb.parser;

import org.csu.mydb.cli.ParsedCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {
    private static final String[] EMPTY_ARRAY = new String[0];

    /**
     * 解析输入的 SQL 字符串为 ParsedCommand 对象（完整实现）
     * @param input 用户输入的 SQL 命令（已转为小写）
     * @return 解析后的 ParsedCommand
     */
    public ParsedCommand parse(String input) {
        ParsedCommand cmd = new ParsedCommand();
        String trimmedInput = input.trim();
        if (trimmedInput.isEmpty()) {
            return cmd;  // 空命令
        }

        String[] tokens = splitString(trimmedInput);  // 按空格分割（支持多空格）

        if (tokens.length == 0) {
            return cmd;
        }

        switch (tokens[0]) {
            case "create":
                handleCreate(tokens, cmd);
                break;
            case "drop":
                handleDrop(tokens, cmd);
                break;
            case "open":
                handleOpen(tokens, cmd);
                break;
            case "close":
                handleClose(tokens, cmd);
                break;
            case "insert":
                handleInsert(tokens, cmd);
                break;
            case "update":
                handleUpdate(tokens, cmd);
                break;
            case "delete":
                handleDelete(tokens, cmd);
                break;
            case "query":
                handleQuery(tokens, cmd);
                break;
            case "exit":
                cmd.setType(ParsedCommand.CommandType.EXIT);
                break;
            default:
                System.out.println("错误：未知命令 '" + tokens[0] + "'");
                cmd.setType(ParsedCommand.CommandType.EXIT);  // 未知命令默认退出
        }
        return cmd;
    }

    /**
     * 处理 CREATE 命令（数据库/表）
     */
    private void handleCreate(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 2) {
            System.out.println("错误：CREATE 命令格式不完整（示例：create database/db table 表名 (列定义)）");
            return;
        }

        switch (tokens[1]) {
            case "database":
                handleCreateDatabase(tokens, cmd);
                break;
            case "table":
                handleCreateTable(tokens, cmd);
                break;
            default:
                System.out.println("错误：CREATE 命令不支持 '" + tokens[1] + "'");
        }
    }

    /**
     * 处理 CREATE DATABASE 子命令
     */
    private void handleCreateDatabase(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3) {
            System.out.println("错误：CREATE DATABASE 格式不完整（示例：create database 数据库名）");
            return;
        }
        cmd.setType(ParsedCommand.CommandType.CREATE_DB);
        cmd.setTarget(tokens[2]);  // 数据库名
        cmd.setParams(EMPTY_ARRAY);  // 无额外参数
    }

    /**
     * 处理 CREATE TABLE 子命令（支持列定义）
     */
    private void handleCreateTable(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3 || !tokens[1].equals("table")) {
            System.out.println("错误：CREATE TABLE 命令格式不正确");
            return;
        }

        // 表名是 tokens[2]
        String tableName = tokens[2];
        cmd.setTarget(tableName);

        // 寻找左括号 '(' 的位置
        int leftParenIndex = -1;
        for (int i = 3; i < tokens.length; i++) {
            if (tokens[i].startsWith("(")) {
                leftParenIndex = i;
                break;
            }
        }
        if (leftParenIndex == -1) {
            System.out.println("错误：CREATE TABLE 缺少左括号 '('");
            return;
        }

        // 寻找右括号 ')' 的位置
        int rightParenIndex = -1;
        for (int i = leftParenIndex; i < tokens.length; i++) {
            if (tokens[i].endsWith(")")) {
                rightParenIndex = i;
                break;
            }
        }
        if (rightParenIndex == -1) {
            System.out.println("错误：CREATE TABLE 缺少右括号 ')'");
            return;
        }

        // 提取括号内的列定义部分（包括括号）
        StringBuilder columnsPartBuilder = new StringBuilder();
        for (int i = leftParenIndex; i <= rightParenIndex; i++) {
            columnsPartBuilder.append(tokens[i]);
            if (i < rightParenIndex) {
                columnsPartBuilder.append(" ");
            }
        }
        String columnsPart = columnsPartBuilder.toString().trim();

        // 去除括号
        if (!columnsPart.startsWith("(") || !columnsPart.endsWith(")")) {
            System.out.println("错误：列定义括号不匹配");
            return;
        }
        columnsPart = columnsPart.substring(1, columnsPart.length() - 1).trim();

        // 按逗号分割列定义（处理逗号后的空格）
        String[] columnDefs = splitByComma(columnsPart);
        cmd.setParams(columnDefs);

        cmd.setType(ParsedCommand.CommandType.CREATE_TABLE);
    }

    /**
     * 处理 DROP 命令（数据库/表）
     */
    private void handleDrop(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 2) {
            System.out.println("错误：DROP 命令格式不完整（示例：drop database/db table 表名）");
            return;
        }

        switch (tokens[1]) {
            case "database":
                handleDropDatabase(tokens, cmd);
                break;
            case "table":
                handleDropTable(tokens, cmd);
                break;
            default:
                System.out.println("错误：DROP 命令不支持 '" + tokens[1] + "'");
        }
    }

    /**
     * 处理 DROP DATABASE 子命令
     */
    private void handleDropDatabase(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3) {
            System.out.println("错误：DROP DATABASE 格式不完整（示例：drop database 数据库名）");
            return;
        }
        cmd.setType(ParsedCommand.CommandType.DROP_DB);
        cmd.setTarget(tokens[2]);  // 数据库名
        cmd.setParams(EMPTY_ARRAY);  // 无额外参数
    }

    /**
     * 处理 DROP TABLE 子命令
     */
    private void handleDropTable(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3) {
            System.out.println("错误：DROP TABLE 格式不完整（示例：drop table 表名）");
            return;
        }
        cmd.setType(ParsedCommand.CommandType.DROP_TABLE);
        cmd.setTarget(tokens[2]);  // 表名
        cmd.setParams(EMPTY_ARRAY);  // 无额外参数
    }

    /**
     * 处理 OPEN 命令（打开数据库）
     */
    private void handleOpen(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3 || !tokens[1].equals("database")) {
            System.out.println("错误：OPEN 命令格式不完整（示例：open database 数据库名）");
            return;
        }
        cmd.setType(ParsedCommand.CommandType.OPEN_DB);
        cmd.setTarget(tokens[2]);  // 数据库名
        cmd.setParams(EMPTY_ARRAY);  // 无额外参数
    }

    /**
     * 处理 CLOSE 命令（关闭数据库）
     */
    private void handleClose(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3 || !tokens[1].equals("database")) {
            System.out.println("错误：CLOSE 命令格式不完整（示例：close database 数据库名）");
            return;
        }
        cmd.setType(ParsedCommand.CommandType.CLOSE_DB);
        cmd.setTarget(tokens[2]);  // 数据库名（可选，可留空）
        cmd.setParams(EMPTY_ARRAY);  // 无额外参数
    }

    /**
     * 处理 INSERT 命令（插入数据）
     */
    private void handleInsert(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 6 || !tokens[1].equals("into") || !tokens[3].startsWith("(") || !tokens[5].startsWith("values")) {
            System.out.println("错误：INSERT 格式不完整（示例：insert into 表名 (列1,列2) values (值1,值2)）");
            return;
        }

        cmd.setType(ParsedCommand.CommandType.INSERT);
        cmd.setTarget(tokens[2]);  // 表名

        // 解析列名（格式：(列1,列2)）
        String columnsPart = tokens[3].substring(1, tokens[3].length() - 1);  // 去除括号
        List<String> columns = Arrays.asList(columnsPart.split(","));
        cmd.setParams(columns.toArray(new String[0]));  // 列名作为参数

        // 解析值（格式：(值1,值2)）
        // 注意：实际需处理字符串转义等复杂情况，此处简化为直接提取
        String valuesPart = tokens[5].substring(7, tokens[5].length() - 1);  // 去除 "values (" 和 ")"
        cmd.setParams(valuesPart.split(","));  // 值作为参数（需根据实际类型转换）
    }

    /**
     * 处理 UPDATE 命令（更新数据）
     */
    private void handleUpdate(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 8 || !tokens[1].equals("set") || !tokens[3].startsWith("where")) {
            System.out.println("错误：UPDATE 格式不完整（示例：update 表名 set 列=值 where 条件）");
            return;
        }

        cmd.setType(ParsedCommand.CommandType.UPDATE);
        cmd.setTarget(tokens[2]);  // 表名

        // 解析 SET 部分（格式：列=值）
        String setPart = tokens[3].substring(4);  // 去除 "set "
        String[] setParts = setPart.split("=");
        if (setParts.length != 2) {
            System.out.println("错误：SET 格式错误（示例：列=值）");
            return;
        }
        cmd.setParams(new String[]{setParts[0].trim(), setParts[1].trim()});  // 列名和新值

        // 解析 WHERE 条件（格式：列=值）
        String wherePart = tokens[5].substring(6);  // 去除 "where "
        String[] whereParts = wherePart.split("=");
        if (whereParts.length != 2) {
            System.out.println("错误：WHERE 格式错误（示例：列=值）");
            return;
        }
        cmd.setParams(new String[]{whereParts[0].trim(), whereParts[1].trim()});  // 条件列和值
    }

    /**
     * 处理 DELETE 命令（删除数据）
     */
    private void handleDelete(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 5 || !tokens[3].startsWith("where")) {
            System.out.println("错误：DELETE 格式不完整（示例：delete from 表名 where 条件）");
            return;
        }

        cmd.setType(ParsedCommand.CommandType.DELETE);
        cmd.setTarget(tokens[2]);  // 表名

        // 解析 WHERE 条件（格式：列=值）
        String wherePart = tokens[5].substring(6);  // 去除 "where "
        String[] whereParts = wherePart.split("=");
        if (whereParts.length != 2) {
            System.out.println("错误：WHERE 格式错误（示例：列=值）");
            return;
        }
        cmd.setParams(new String[]{whereParts[0].trim(), whereParts[1].trim()});  // 条件列和值
    }

    /**
     * 处理 QUERY 命令（查询数据）
     */
    private void handleQuery(String[] tokens, ParsedCommand cmd) {
        if (tokens.length < 3 || !tokens[1].equals("from")) {
            System.out.println("错误：QUERY 格式不完整（示例：query 列名 from 表名 [where 条件]）");
            return;
        }

        cmd.setType(ParsedCommand.CommandType.SELECT);

        // 解析查询列（"all" 或具体列名）
        if (tokens[2].equalsIgnoreCase("all")) {
            cmd.setTarget("all");  // 标记为查询所有列
        } else {
            cmd.setTarget(tokens[2]);  // 具体列名（支持多列，如 "col1,col2"）
        }

        // 解析表名（格式：from 表名）
        cmd.setTarget(tokens[4]);  // 表名（覆盖之前的列名）

        // 解析 WHERE 条件（可选）
        if (tokens.length > 6 && tokens[5].startsWith("where")) {
            String wherePart = tokens[7].substring(6);  // 去除 "where "
            String[] whereParts = wherePart.split("=");
            if (whereParts.length == 2) {
                cmd.setParams(new String[]{whereParts[0].trim(), whereParts[1].trim()});  // 条件列和值
            }
        }
    }

    /**
     * 分割输入字符串为 token（按空格分割，支持多空格）
     * @param input 输入字符串
     * @return 分割后的 token 数组
     */
    private String[] splitString(String input) {
        // 按任意数量空格分割，并过滤空字符串
        System.out.println(Arrays.toString(Arrays.stream(input.split("\\s+"))
                .filter(token -> !token.isEmpty())
                .toArray(String[]::new)));
        return Arrays.stream(input.split("\\s+"))
                .filter(token -> !token.isEmpty())
                .toArray(String[]::new);
    }

    // 在 Parser 类中新增方法
    private String[] splitByComma(String input) {
        // 按逗号分割，去除前后空格
        return Arrays.stream(input.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())  // 过滤空字符串
                .toArray(String[]::new);
    }
}
