package org.csu.mydb.log;

import java.util.Map;
import java.util.Set;

public class LoggerManager {

    /**
     * 将[log]节的配置转换为Logback需要的系统属性
     */
    public static void setLogbackProperties(Map<String, String> logConfig) {
        // 日志路径
        String logPath = logConfig.getOrDefault("path", "logs/mydb.log");
        System.setProperty("log.path", logPath);

        // 日志级别（默认：INFO，需校验有效性）
        String logLevel = logConfig.getOrDefault("level", "INFO").toUpperCase();
        Set<String> validLevels = Set.of("DEBUG", "INFO", "WARN", "ERROR");
        if (validLevels.contains(logLevel)) {
            System.setProperty("log.level", logLevel);
        } else {
            System.err.printf("警告：无效的日志级别 '%s'，使用默认值 INFO%n", logLevel);
            System.setProperty("log.level", "INFO");
        }
    }
}
