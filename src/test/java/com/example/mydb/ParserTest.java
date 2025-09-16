package com.example.mydb;

import org.csu.mydb.compiler.Grammar;
import org.csu.mydb.compiler.Lexer;
import org.csu.mydb.compiler.Parser;

import java.util.List;

/**
 * * @author ljyljy
 * @date 2025/9/12
 * @description 语法分析器测试 把需要测试的语句放到sql中
  * 支持的格式 其他的格式可能识别不出来
  * CREATE DATABASE db;
  * use db;
  * create table student(
  * id varchar(20)
  * )
  * create table teacher(
  * age INT primary KEY,
  * id VARCHAR(10)
  * )
  * create table game(
  * price not null
  * )
  * SELECT id from student where name = 'dd';
  * select EVERYTHING from student ;
  * select id from student where name = 'dd' AND id = 3;
  * insert into student values (1,'33');
  * insert into student (id , age) VALUES (1,4);
  * UPDATE student set id = 5 where age = 6;
  * UPDATE student set id = 5 where age = 6 and grade = 'dd';
  * delete from student where age = 5;
  * delete from student where age = 6 AND id = 9;
  * drop table student;
  * DROP DATABASE db;
  */

public class ParserTest {
    public static void main(String[] args) {
        Grammar g = new Grammar();
        g.computeFirstSets();
        g.computeFollowSets();

        Parser parser = new Parser(g);
        parser.printParseTable();

        // 测试示例 SQL
        String sql = "insert into student values (6,'ff');";
        List<Lexer.Token> tokens = Lexer.tokenize(sql);

        parser.parse(tokens);
    }
}
