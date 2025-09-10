package org.csu.mydb.config;

import ch.qos.logback.classic.LoggerContext;
import org.csu.mydb.Main;
import org.csu.mydb.log.LoggerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigLoader {

    // 是否已经加载
    private boolean isLoaded = false;

    private static final Properties props = new Properties();

    // 单例实例（静态内部类保证线程安全）
    private static class Holder {
        static final ConfigLoader INSTANCE = new ConfigLoader();
    }

    // 配置存储结构：[section] → {key: value}
    private final Map<String, Map<String, String>> configData = new ConcurrentHashMap<>();

    // 私有构造函数（防止外部实例化）
    private ConfigLoader() {
    }

    /**
     * 获取单例实例（线程安全）
     */
    public static ConfigLoader getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 异步加载配置文件
     * @param configPath 配置文件相对位置
     */
    public CompletableFuture<Void> loadConfigAsync(String configPath) {
        return CompletableFuture.runAsync(() -> {
            try {
                // 1. 加载配置文件
                loadConfig(configPath);  // 原有同步加载逻辑

                // 2. 读取日志配置并设置系统属性（关键步骤）
                Map<String, String> logConfig = getSection("log");
                LoggerManager.setLogbackProperties(logConfig);

                // 3. 初始化日志框架（Logback会自动读取系统属性）
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                context.reset(); // 重置默认配置（可选，确保加载最新属性）
            } catch (IOException e) {
                throw new RuntimeException("异步加载配置失败", e);
            }
        });
    }

    /**
     * 加载配置文件（程序启动时调用）
     * @param configPath 配置文件路径（默认从 resource/mydb.ini 读取）
     */
    public void loadConfig(String configPath) throws IOException {
        // 已加载则跳过
        if (isLoaded) {
            System.out.println("配置已加载，无需重复加载");
            return;
        }

        // 使用类加载器获取资源流（支持 IDE 和打包后的路径）
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configPath);
        if (inputStream == null) {
            throw new IOException("配置文件未找到: " + configPath);
        }

        // 解析 INI 文件
        parseIniFile(inputStream);

        isLoaded = true;  // 标记为已加载
    }

    /**
     * 解析 INI 文件内容
     */
    private void parseIniFile(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String originalLine;       // 保存原始行（用于错误提示）
            String currentSectionName = null; // 当前所在节的名称（如 "storage"）
            int lineNumber = 0;        // 行号计数器

            while ((originalLine = reader.readLine()) != null) {
                lineNumber++;
                String line = cleanLine(originalLine);  // 清理行内注释和首尾空格

                // 跳过空行（清理后的行可能为空）
                if (line.isEmpty()) {
                    continue;
                }

                // 处理节声明（如 [storage]）
                if (line.startsWith("[") && line.endsWith("]")) {
                    currentSectionName = line.substring(1, line.length() - 1).trim().toLowerCase();
                    configData.putIfAbsent(currentSectionName, new ConcurrentHashMap<>());
                    continue;
                }

                // 处理键值对（必须包含 =）
                int equalPos = line.indexOf('=');
                if (equalPos == -1) {
                    System.err.printf("警告：配置文件第 %d 行格式错误（缺少 =）: %s%n", lineNumber, originalLine);
                    continue;
                }

                // 提取键和值（去除首尾空格）
                String key = line.substring(0, equalPos).trim().toLowerCase();
                String value = line.substring(equalPos + 1).trim();

                if (key.isEmpty() || value.isEmpty()) {
                    System.err.printf("警告：配置文件第 %d 行键或值为空: %s%n", lineNumber, originalLine);
                    continue;
                }

                // 存储到当前节（仅当节存在时）
                if (currentSectionName != null) {
                    configData.get(currentSectionName).put(key, value);
                } else {
                    System.err.printf("警告：配置文件第 %d 行无所属节: %s%n", lineNumber, originalLine);
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

    /**
     * 获取指定节的配置
     * @param sectionName 配置项
     * @return 配置项设置
     */
    public Map<String, String> getSection(String sectionName) {
        if (!isLoaded) {
            throw new IllegalStateException("配置未加载，请先调用loadConfig()");
        }
        // 转换为小写
        String normalizedSection = sectionName.toLowerCase();
        return configData.getOrDefault(normalizedSection, Collections.emptyMap());
    }
}