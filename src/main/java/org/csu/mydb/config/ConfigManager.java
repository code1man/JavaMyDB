package org.csu.mydb.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigManager {
    // 单例实例（静态内部类保证线程安全）
    private static class Holder {
        static final ConfigManager INSTANCE = new ConfigManager();
    }

    // 配置存储结构：[section] → {key: value}
    private final Map<String, Map<String, String>> configData = new ConcurrentHashMap<>();

    // 私有构造函数（防止外部实例化）
    private ConfigManager() {
        // 初始化时加载默认配置（可选）
        loadDefaultConfig();
    }

    /**
     * 获取单例实例（线程安全）
     */
    public static ConfigManager getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 加载配置文件（程序启动时调用）
     * @param configPath 配置文件路径（默认从 resource/mydb.ini 读取）
     */
    public void loadConfig(String configPath) throws IOException {
        // 使用类加载器获取资源流（支持 IDE 和打包后的路径）
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configPath);
        if (inputStream == null) {
            throw new IOException("配置文件未找到: " + configPath);
        }

        // 解析 INI 文件
        parseIniFile(inputStream);
    }

    /**
     * 解析 INI 文件内容
     */
    private void parseIniFile(InputStream inputStream) throws IOException {
        try (Scanner scanner = new Scanner(inputStream)) {
            String currentSection = null;
            int lineNumber = 0;

            while (scanner.hasNextLine()) {
                lineNumber++;
                String originalLine = scanner.nextLine();
                String line = cleanLine(originalLine);  // 清理行内注释和首尾空格

                // 跳过空行（清理后的行可能为空）
                if (line.isEmpty()) {
                    continue;
                }

                // 处理节声明（如 [storage]）
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSection = line.substring(1, line.length() - 1).trim().toLowerCase();
                    configData.putIfAbsent(currentSection, new ConcurrentHashMap<>());
                    continue;
                }

                // 处理键值对（如 page_size=4096）
                int equalPos = line.indexOf('=');
                if (equalPos == -1) {
                    System.err.println("警告：配置文件第 " + lineNumber + " 行格式错误（缺少 =）: " + originalLine);
                    continue;
                }

                String key = line.substring(0, equalPos).trim().toLowerCase();
                String value = line.substring(equalPos + 1).trim();

                if (key.isEmpty() || value.isEmpty()) {
                    System.err.println("警告：配置文件第 " + lineNumber + " 行键或值为空: " + originalLine);
                    continue;
                }

                // 存储到当前节
                if (currentSection != null) {
                    configData.get(currentSection).put(key, value);
                } else {
                    System.err.println("警告：配置文件第 " + lineNumber + " 行无所属节: " + originalLine);
                }
            }
        }
    }

    /**
     * 彻底清理行内注释和首尾空格（关键修复）
     */
    private String cleanLine(String line) {
        // 1. 处理行首注释（; 或 # 开头）
        if (line.startsWith(";") || line.startsWith("#")) {
            return "";  // 整行注释，返回空字符串
        }

        // 2. 处理行内注释（取第一个 ; 或 # 的位置）
        int commentIndex = -1;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == ';' || c == '#') {
                commentIndex = i;
                break;
            }
        }

        // 3. 截断注释前的内容并去首尾空格
        String cleanedLine;
        if (commentIndex != -1) {
            cleanedLine = line.substring(0, commentIndex).trim();
        } else {
            cleanedLine = line.trim();
        }

        // 4. 若清理后为空，返回空字符串（跳过空行）
        return cleanedLine.isEmpty() ? "" : cleanedLine;
    }

    /**
     * 加载默认配置（可选，用于补充缺失的配置项）
     */
    private void loadDefaultConfig() {
        // 示例默认配置（可根据需求扩展）
        Map<String, Map<String, String>> defaultConfig = new HashMap<>();

        // 存储模块默认配置
        Map<String, String> storageConfig = new HashMap<>();
        storageConfig.put("page_size", "4096");
        storageConfig.put("buffer_pool_size", "100");
        storageConfig.put("max_connections", "1000");
        defaultConfig.put("storage", storageConfig);

        // 日志模块默认配置
        Map<String, String> logConfig = new HashMap<>();
        logConfig.put("level", "INFO");
        logConfig.put("path", "logs/mydb.log");
        defaultConfig.put("log", logConfig);

        // 合并到主配置（优先使用用户配置，无则用默认）
        defaultConfig.forEach((section, keys) -> {
            configData.putIfAbsent(section, new ConcurrentHashMap<>());
            keys.forEach((key, value) -> {
                if (!configData.get(section).containsKey(key)) {
                    configData.get(section).put(key, value);
                }
            });
        });
    }

    /**
     * 获取字符串类型配置（支持默认值）
     */
    public String getString(String section, String key, String defaultValue) {
        return configData.getOrDefault(section, Collections.emptyMap())
                .getOrDefault(key.toLowerCase(), defaultValue);
    }

    /**
     * 获取整数类型配置（支持默认值，转换失败返回默认值）
     */
    public int getInt(String section, String key, int defaultValue) {
        String value = getString(section, key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            System.err.println("警告：配置项 " + section + "." + key + " 格式错误（期望整数）: " + value);
            return defaultValue;
        }
    }

    /**
     * 获取布尔类型配置（支持默认值，转换失败返回默认值）
     */
    public boolean getBool(String section, String key, boolean defaultValue) {
        String value = getString(section, key, String.valueOf(defaultValue)).toLowerCase();
        return value.equals("true") || value.equals("1");
    }

    /**
     * 检查配置项是否存在
     */
    public boolean contains(String section, String key) {
        return configData.containsKey(section) && configData.get(section).containsKey(key.toLowerCase());
    }
}