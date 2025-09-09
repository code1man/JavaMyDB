package org.csu.mydb;

import org.csu.mydb.executor.Executor;
import org.csu.mydb.parser.Parser;
import org.csu.mydb.storage.StorageEngine;
import org.csu.mydb.cli.CLI;


public class Main {
    public static void main(String[] args) {
        // 初始化模块
        StorageEngine storageEngine = new StorageEngine();
        Parser parser = new Parser();
        Executor executor = new Executor(storageEngine);
        CLI cli = new CLI(parser, executor);

        // 启动交互循环
        cli.start();
    }
}