package org.csu.mydb.cli;

import java.util.Arrays;

public class ParsedCommand {
    // 命令类型枚举
    public enum CommandType {
        CREATE_DB, DROP_DB, OPEN_DB, CLOSE_DB,
        CREATE_TABLE, DROP_TABLE, INSERT, UPDATE, DELETE, SELECT, EXIT
    }

    private CommandType type;       // 命令类型
    private String target;          // 目标（数据库名/表名）
    private String[] params;        // 其他参数（列名、值等）

    // Getter 和 Setter
    public CommandType getType() { return type; }
    public void setType(CommandType type) { this.type = type; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }

    public String[] getParams() { return params; }
    public void setParams(String[] params) { this.params = params; }

    @Override
    public String toString() {
        return "ParsedCommand{" +
                "type=" + type +
                ", target='" + target + '\'' +
                ", params=" + Arrays.toString(params) +
                '}';
    }
}