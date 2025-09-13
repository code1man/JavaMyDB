package com.example.mydb;
import org.csu.mydb.compiler.Lexer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.csu.mydb.compiler.Lexer.tokenize;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description 词法分析器的测试
 * */
public class LexerTest {
    // main：从文件读取或使用内置示例，输出 token 到控制台或文件
    public static void main(String[] args) {
        String input = null;
        if (args.length >= 1) {
            try {
                input = Files.readString(Paths.get(args[0]), StandardCharsets.UTF_8);
            } catch (Exception e) {
                System.err.println("读取文件出错：" + e.getMessage());
                return;
            }
        } else {
            // 默认示例（若未提供文件）
            input = ""
                    + "/* 示例 SQL */\n"
                    + "CREATE DATABASE test_db;\n"
                    + "CREATE TABLE users (id INT, name VARCHAR(100));\n"
                    + "INSERT INTO users (id, name) VALUES (1, 'Alice');\n"
                    + "SELECT id, name FROM users WHERE id >= 1 AND name != 'Bob';\n"
                    + "-- 结束\n";
            System.out.println("未提供输入文件，使用内置示例：\n" + input + "\n---- 分析结果 ----");
        }

        List<Lexer.Token> tokens = tokenize(input);

        // 输出四元式到控制台
        System.out.println("种别码\t词素\t行:列");
        for (Lexer.Token tk : tokens) {
            System.out.println(tk.toString());
        }

        // 如果用户提供第二个参数则把 token 写入该文件
        if (args.length >= 2) {
            try {
                Path out = Paths.get(args[1]);
                List<String> lines = new ArrayList<>();
                lines.add("种别码\t词素\t行:列");
                for (Lexer.Token tk : tokens) lines.add(tk.toString());
                Files.write(out, lines, StandardCharsets.UTF_8);
                System.out.println("token 已写入 " + args[1]);
            } catch (Exception e) {
                System.err.println("写入输出文件出错：" + e.getMessage());
            }
        }
    }
}
