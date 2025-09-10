package org.csu.mydb;

import org.csu.mydb.config.ConfigManager;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.parser.Parser;
import org.csu.mydb.storage.StorageEngine;
import org.csu.mydb.cli.CLI;

import java.io.IOException;


public class Main {
    public static void main(String[] args) {
        try {
            // 加载配置文件（路径相对于 resource 目录）
            ConfigManager.getInstance().loadConfig("config/mydb.ini");
            System.out.println("配置加载成功！");
        } catch (IOException e) {
            System.err.println("配置加载失败: " + e.getMessage());
            System.exit(1);  // 配置缺失时终止程序
        }

        // 初始化模块
        StorageEngine storageEngine = new StorageEngine();
        Parser parser = new Parser();
        Executor executor = new Executor(storageEngine);
        CLI cli = new CLI(parser, executor);

        // 启动交互循环
        cli.start();
    }
}