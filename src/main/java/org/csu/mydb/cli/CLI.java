package org.csu.mydb.cli;

import org.csu.mydb.executor.Executor;
import org.csu.mydb.parser.Parser;

import java.util.Scanner;

public class CLI {
    private final Parser parser;
    private final Executor executor;
    private final Scanner scanner = new Scanner(System.in);

    public CLI(Parser parser, Executor executor) {
        this.parser = parser;
        this.executor = executor;
    }

    public void start() {
        printWelcome();
        boolean shouldContinue = true;
        while (shouldContinue) {
            printPrompt();
            String input = readInput();
            if (input.trim().isEmpty()) {
                continue;
            }
            // 解析命令并执行
            ParsedCommand command = parser.parse(input.trim().toLowerCase());
            shouldContinue = executor.execute(command);
        }
        scanner.close();
    }

    private void printWelcome() {
        System.out.println("欢迎使用 MyDB 小型数据库系统！");
        System.out.println("支持的命令（不区分大小写）：");
        System.out.println("  创建数据库: create database 数据库名");
        System.out.println("  删除数据库: drop database 数据库名");
        System.out.println("  打开数据库: open database 数据库名");
        System.out.println("  关闭数据库: close database 数据库名");
        System.out.println("  创建表    : create table 表名");
        System.out.println("            (");
        System.out.println("              列1 名称 数据类型(长度可选),");
        System.out.println("              列2 名称 数据类型(长度可选),");
        System.out.println("              ...");
        System.out.println("            )");
        System.out.println("  删除表    : drop table 表名");
        System.out.println("  查看全表  : query all from 表名");
        System.out.println("  查询单值  : query 列名 from 表名 where 列名 = 值");
        System.out.println("  插入数据  : insert into 表名(列1,列2...) values(值1,值2...)");
        System.out.println("  修改数据  : update 表名 set 列名=新值 where 列名=值");
        System.out.println("  删除数据  : delete from 表名 where 列名=值");
        System.out.println("  退出程序  : exit");
    }

    private void printPrompt() {
        System.out.print("my_db> ");
    }

    private String readInput() {
        return scanner.nextLine();
    }
}