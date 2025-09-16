package org.csu.mydb;

import org.csu.mydb.compiler.Grammar;
import org.csu.mydb.config.ConfigLoader;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.compiler.Parser;
import org.csu.mydb.storage.StorageEngine;
import org.csu.mydb.cli.CLI;
import org.csu.mydb.storage.StorageSystem;

public class Main {
    public static void main(String[] args) {
        // 异步读取配置文件
        ConfigLoader.getInstance().loadConfigAsync("config/mydb.ini")
                .exceptionally(e -> {
                    System.err.println("配置加载失败: " + e.getMessage());
                    System.exit(1);
                    return null;
                });

        // 初始化模块
        StorageEngine storageEngine = new StorageEngine(new StorageSystem().getPageManager());
        Parser parser = new Parser(new Grammar());
        Executor executor = new Executor(storageEngine);
        CLI cli = new CLI(parser, executor);

        // 启动交互循环
        cli.start();
    }
}