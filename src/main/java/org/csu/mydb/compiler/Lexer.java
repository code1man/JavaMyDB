package org.csu.mydb.compiler;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description 词法分析器
 */
public class Lexer {
    // token 类别码
    public static final int KEYWORD = 1;
    public static final int IDENTIFIER = 2;
    public static final int CONSTANT = 3;
    public static final int OPERATOR = 4;
    public static final int DELIMITER = 5;
    public static final int STRING = 6;
    // 关键词集合（大写存储，匹配时忽略大小写）
    private static final Set<String> KEYWORDS = new HashSet<>(Arrays.asList(
            "SELECT","FROM","WHERE","CREATE","TABLE","INSERT","INTO","VALUES",
            "UPDATE","SET","DELETE","DROP","DATABASE","USE",
            "AND","NOT","PRIMARY","KEY","INT","VARCHAR","CHAR","EVERYTHING","NULL"
    ));

    // 两字符运算符优先表
    private static final Set<String> TWO_CHAR_OPS = new HashSet<>(Arrays.asList(
            ">=","<=", "!=", "<>", "=="
    ));

    // 单字符运算符/界符（交给判断时使用）
    private static final Set<Character> SINGLE_OPERATORS = new HashSet<>(Arrays.asList(
            '+','-','*','/','%','=','>','<'
    ));

    private static final Set<Character> DELIMITERS = new HashSet<>(Arrays.asList(
            ',', ';', '.', '(', ')', '{', '}'
    ));

    // 词法单元类
    public static class Token {
        public final int type;
        public final String lexeme;
        public final int line;
        public final int column;

        public Token(int type, String lexeme, int line, int column) {
            this.type = type;
            this.lexeme = lexeme;
            this.line = line;
            this.column = column;
        }

        @Override
        public String toString() {
            // 输出格式：单词种别码 \t 单词的值 \t 行:列
            return String.format("%d\t%s\t%d:%d", type, lexeme, line, column);
        }
    }

    // 词法分析主函数：接收输入字符串，返回 token 列表
    public static List<Token> tokenize(String input) {
        List<Token> tokens = new ArrayList<>();
        List<String> identifierTable = new ArrayList<>(); // 可扩展为 symbol table
        List<String> constantTable = new ArrayList<>();

        int i = 0;
        int len = input.length();
        int line = 1;
        int col = 1;

        while (i < len) {
            char ch = input.charAt(i);

            // 换行处理（支持 \r\n 与 \n）
            if (ch == '\r') { i++; continue; }
            if (ch == '\n') { i++; line++; col = 1; continue; }

            // 空白跳过
            if (Character.isWhitespace(ch)) {
                i++; col++;
                continue;
            }

            // 注释：-- 到行尾
            if (ch == '-' && i + 1 < len && input.charAt(i + 1) == '-') {
                i += 2; col += 2;
                while (i < len && input.charAt(i) != '\n') { i++; col++; }
                continue;
            }

            // 注释：/* ... */
            if (ch == '/' && i + 1 < len && input.charAt(i + 1) == '*') {
                int startLine = line, startCol = col;
                i += 2; col += 2;
                boolean closed = false;
                while (i + 1 < len) {
                    if (input.charAt(i) == '*' && input.charAt(i + 1) == '/') {
                        i += 2; col += 2;
                        closed = true;
                        break;
                    } else {
                        if (input.charAt(i) == '\n') { i++; line++; col = 1; }
                        else { i++; col++; }
                    }
                }
                if (!closed) {
                    System.err.printf("未闭合的多行注释，开始于 %d:%d%n", startLine, startCol);
                }
                continue;
            }

            // 字符串字面量（SQL 使用单引号，支持 '' 作为转义）
            if (ch == '\'') {
                int startLine = line, startCol = col;
                StringBuilder sb = new StringBuilder();
                sb.append(ch);
                i++; col++;
                boolean closed = false;
                while (i < len) {
                    char c2 = input.charAt(i);
                    sb.append(c2);
                    i++; col++;
                    if (c2 == '\'') {
                        // 如果是两个连续的单引号，表示转义，继续
                        if (i < len && input.charAt(i) == '\'') {
                            sb.append('\'');
                            i++; col++;
                            continue;
                        } else {
                            closed = true;
                            break;
                        }
                    }
                    if (c2 == '\n') { line++; col = 1; }
                }
                if (!closed) {
                    System.err.printf("字符串未闭合，开始于 %d:%d%n", startLine, startCol);
                }
                String lit = sb.toString();
                tokens.add(new Token(STRING, lit, startLine, startCol)); //  用 STRING 类型
                continue;
            }


            // 标识符或关键字（字母或下划线开头，后续可有字母数字下划线）
            if (Character.isLetter(ch) || ch == '_') {
                int startLine = line, startCol = col;
                StringBuilder sb = new StringBuilder();
                sb.append(ch);
                i++; col++;
                while (i < len) {
                    char c2 = input.charAt(i);
                    if (Character.isLetterOrDigit(c2) || c2 == '_') {
                        sb.append(c2);
                        i++; col++;
                    } else break;
                }
                String word = sb.toString();
                String up = word.toUpperCase();
                if (KEYWORDS.contains(up)) {
                    tokens.add(new Token(KEYWORD, word, startLine, startCol));
                } else {
                    tokens.add(new Token(IDENTIFIER, word, startLine, startCol));
                    if (!identifierTable.contains(word)) identifierTable.add(word);
                }
                continue;
            }

            // 数字常量（整数或小数），若以数字开头则把后续点和数字也吞进来
            if (Character.isDigit(ch)) {
                int startLine = line, startCol = col;
                StringBuilder sb = new StringBuilder();
                sb.append(ch);
                i++; col++;
                boolean hasDot = false;
                while (i < len) {
                    char c2 = input.charAt(i);
                    if (Character.isDigit(c2)) {
                        sb.append(c2); i++; col++;
                    } else if (c2 == '.' && !hasDot) {
                        // 可能是小数点，但要保证后面是数字
                        if (i + 1 < len && Character.isDigit(input.charAt(i + 1))) {
                            hasDot = true;
                            sb.append(c2); i++; col++;
                        } else {
                            break; // 点不是数字的一部分（例如 schema.table 的点）
                        }
                    } else break;
                }
                String num = sb.toString();
                tokens.add(new Token(CONSTANT, num, startLine, startCol));
                if (!constantTable.contains(num)) constantTable.add(num);
                continue;
            }

            // 两字符运算符检查（优先）
            if (i + 1 < len) {
                String two = "" + ch + input.charAt(i + 1);
                if (TWO_CHAR_OPS.contains(two)) {
                    tokens.add(new Token(OPERATOR, two, line, col));
                    i += 2; col += 2;
                    continue;
                }
            }

            // 单字符运算符
            if (SINGLE_OPERATORS.contains(ch)) {
                // / 既可能是注释起始（上面已处理），也可能是除法运算符
                tokens.add(new Token(OPERATOR, String.valueOf(ch), line, col));
                i++; col++;
                continue;
            }

            // 界符
            if (DELIMITERS.contains(ch)) {
                tokens.add(new Token(DELIMITER, String.valueOf(ch), line, col));
                i++; col++;
                continue;
            }

            // 未知字符（可以扩展）
            System.err.printf("发现未识别字符 '%c' 在 %d:%d，跳过%n", ch, line, col);
            i++; col++;
        }

        return tokens;
    }
}
