package com.example.mydb;

import org.csu.mydb.compiler.Lexer;
import org.csu.mydb.compiler.PlanBuilder;
import org.csu.mydb.executor.ExecutionPlan;
import org.csu.mydb.executor.ExecutionResult;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.executor.ExecutorException;

import java.util.List;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description PlanBuilderTest
 */
public class PlanBuilderTest {
    // 1) 从 lexer 获取 tokens
    public static void main(String[] args) throws PlanBuilder.SemanticException, ExecutorException {
        List<Lexer.Token> tokens = Lexer.tokenize("CREATE DATABASE db;\n" +
                "use db;\n" +
                "create table student(\n" +
                "id varchar(20)\n" +
                ")\n" +
                "create table teacher(\n" +
                "age INT primary KEY,\n" +
                "id VARCHAR(10)\n" +
                ") \n" +
                "create table game(\n" +
                "price int not null\n" +
                ")\n" +
                "SELECT id from student where name = 'dd';\n" +
                "select anything from student ;\n" +
                "select id from student where name = 'dd' AND id = 3;\n" +
                "insert into student values (1,'33');\n" +
                "insert into student (id , age) VALUES (1,4);\n" +
                "UPDATE student set id = 5 where age = 6;\n" +
                "UPDATE student set id = 5 where age = 6 and grade = 'dd';\n" +
                "delete from student where age = 5;\n" +
                "delete from student where age = 6 AND id = 9;\n" +
                "drop table student;\n" +
                "DROP DATABASE db;");

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

}
