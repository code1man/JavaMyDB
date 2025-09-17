package org.csu.mydb.cli;

import org.csu.mydb.compiler.Grammar;
import org.csu.mydb.compiler.Lexer;
import org.csu.mydb.compiler.PlanBuilder;
import org.csu.mydb.executor.ExecutionPlan;
import org.csu.mydb.executor.ExecutionResult;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.executor.ExecutorException;
import org.csu.mydb.compiler.Parser;
import org.csu.mydb.compiler.Parser;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Scanner;
import java.util.logging.Logger;

import static org.csu.mydb.compiler.Lexer.tokenize;

public class CLI {
    private final Parser parser;
    private final Executor executor;
    private final Scanner scanner = new Scanner(System.in);
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(CLI.class);
    public CLI(Parser parser, Executor executor) {
        this.parser = parser;
        this.executor = executor;
    }

    public void start() throws PlanBuilder.SemanticException, ExecutorException {
        //在程序启动时将产生式全部生成 用于后续语义分析
        Grammar g = new Grammar();
        System.out.println("=== 产生式 ===");
        for (Grammar.NonTerminal nt : g.nonTerminals) {
            List<Grammar.Production> ps = g.getProductions(nt);
            for (Grammar.Production p : ps) System.out.println(p);
        }
        g.computeFirstSets();
        g.computeFollowSets();
        g.printFirstSets();
        g.printFollowSets();
        printWelcome();
        boolean shouldContinue = true;
        while (shouldContinue) {
            printPrompt();
            String input = readInput();
            if (input.trim().isEmpty()) {
                continue;
            }

            // 解析命令并执行
            List<Lexer.Token> tokens = tokenize(input);
            // 词法分析 输出四元式到日志文件
            logger.info("种别码\t词素\t行:列");
            for (Lexer.Token tk : tokens) {
                logger.info(tk.toString());
            }

            parser.printParseTable();
            //检查是否通过语法分析
            parser.parse(tokens);
            //执行计划生成
            // 2) 构建 plan
            PlanBuilder pb = new PlanBuilder();
            List<ExecutionPlan> plans = pb.buildAll(tokens);

            // 3) 给 Executor 执行
            Executor exec = new Executor();

            for (ExecutionPlan plan : plans) {
                ExecutionResult res = exec.execute(plan);
                // 打印或处理结果
            }
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