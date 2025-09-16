package org.csu.mydb.compiler;

import java.util.*;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description 语法分析器
 */
public class Parser {
    private final Grammar grammar;
    private final Map<Grammar.NonTerminal, Map<Grammar.Terminal, Grammar.Production>> parseTable = new HashMap<>();


    // 构造函数
    public Parser() {
        grammar = new Grammar();
        buildParseTable();
    }
    public Parser(Grammar g) {
        this.grammar = g;
        buildParseTable();
    }

    // 构造 LL(1) 预测分析表
    private void buildParseTable() {
        for (Grammar.NonTerminal nt : grammar.nonTerminals) {
            parseTable.put(nt, new HashMap<>());
            List<Grammar.Production> prods = grammar.getProductions(nt);
            for (Grammar.Production p : prods) {
                Set<Grammar.Terminal> firstSet = firstOfSequence(p.right);
                for (Grammar.Terminal t : firstSet) {
                    if (!t.equals(Grammar.EPSILON)) {
                        parseTable.get(nt).put(t, p);
                    }
                }
                if (firstSet.contains(Grammar.EPSILON)) {
                    Set<Grammar.Terminal> followSet = grammar.getFollow(nt);
                    for (Grammar.Terminal t : followSet) {
                        parseTable.get(nt).put(t, p);
                    }
                }
            }
        }
    }

    // 计算右部符号串的 First 集
    private Set<Grammar.Terminal> firstOfSequence(List<Object> symbols) {
        Set<Grammar.Terminal> result = new HashSet<>();
        boolean allEpsilon = true;
        for (Object sym : symbols) {
            if (sym instanceof Grammar.Terminal) {
                Grammar.Terminal t = (Grammar.Terminal) sym;
                if (!t.equals(Grammar.EPSILON)) result.add(t);
                else continue;
                allEpsilon = false;
                break;
            } else if (sym instanceof Grammar.NonTerminal) {
                Grammar.NonTerminal nt = (Grammar.NonTerminal) sym;
                Set<Grammar.Terminal> fset = grammar.getFirst(nt);
                for (Grammar.Terminal t : fset) {
                    if (!t.equals(Grammar.EPSILON)) result.add(t);
                }
                if (!fset.contains(Grammar.EPSILON)) {
                    allEpsilon = false;
                    break;
                }
            }
        }
        if (allEpsilon) result.add(Grammar.EPSILON);
        return result;
    }

    // 打印预测分析表
    public void printParseTable() {
        System.out.println("=== LL(1) 预测分析表 ===");
        for (Grammar.NonTerminal nt : parseTable.keySet()) {
            Map<Grammar.Terminal, Grammar.Production> row = parseTable.get(nt);
            for (Grammar.Terminal t : row.keySet()) {
                System.out.printf("M[%s,%s] = %s%n", nt, t, row.get(t));
            }
        }
    }

    // 语法分析器：输入 token 序列，返回是否通过
    public boolean parse(List<Lexer.Token> tokens) {
        Stack<Object> stack = new Stack<>();
        stack.push(new Grammar.Terminal("$")); // 栈底
        Grammar.NonTerminal start = new Grammar.NonTerminal("Program");
        stack.push(start);

        // 在 token 序列尾部加上 $
        List<Lexer.Token> input = new ArrayList<>(tokens);
        input.add(new Lexer.Token(-1, "$", -1, -1));

        int ip = 0;
        while (!stack.isEmpty()) {
            Object top = stack.pop();
            Lexer.Token currentToken = input.get(ip);
            Grammar.Terminal currTerm = tokenToTerminal(currentToken);

            if (top instanceof Grammar.Terminal) {
                Grammar.Terminal ttop = (Grammar.Terminal) top;
                if (ttop.equals(Grammar.EPSILON)) continue; // ε 直接弹出
                if (ttop.equals(currTerm)) {
                    ip++; // 匹配成功，读取下一个 token
                } else {
                    System.err.printf("语法错误：期望 %s，实际 %s 在 %d:%d%n",
                            ttop, currentToken.lexeme, currentToken.line, currentToken.column);
                    return false;
                }
            } else if (top instanceof Grammar.NonTerminal) {
                Grammar.NonTerminal nt = (Grammar.NonTerminal) top;
                Grammar.Production p = parseTable.get(nt).get(currTerm);
                if (p != null) {
                    // 右部逆序入栈
                    List<Object> rhs = p.right;
                    for (int i = rhs.size() - 1; i >= 0; i--) stack.push(rhs.get(i));
                } else {
                    System.err.printf("语法错误：没有规则可用 M[%s,%s] 在 %d:%d%n",
                            nt, currTerm, currentToken.line, currentToken.column);
                    return false;
                }
            }
        }

        System.out.println("语法分析成功！");
        return true;
    }
    private Grammar.Terminal tokenToTerminal(Lexer.Token tk) {
        if (tk == null) return new Grammar.Terminal("$");

        // 1) 优先根据 token.type 映射成文法终结符名称（稳定、明确）
        if (tk.type == Lexer.IDENTIFIER) {
            return new Grammar.Terminal("IDENTIFIER");
        }
        if (tk.type == Lexer.CONSTANT) {
            return new Grammar.Terminal("CONSTANT");
        }
        // if (tk.type == Lexer.STRING) return new Grammar.Terminal("CONSTANT");

        // 2) 关键字 / 运算符 / 分隔符：直接用字面（大写）作为终结符名
        if (tk.type == Lexer.KEYWORD || tk.type == Lexer.OPERATOR || tk.type == Lexer.DELIMITER) {
            return new Grammar.Terminal(tk.lexeme.toUpperCase());
        }

        // 3) 输入结束符（如果你在 token 序列末端插入 -1 之类）
        if (tk.type == -1) {
            return new Grammar.Terminal("$");
        }

        // 4) 保底：若 lexeme 以单引号包围（'...')，当作常量处理
        String lex = tk.lexeme;
        if (lex != null && lex.length() >= 2 && lex.charAt(0) == '\'' && lex.charAt(lex.length() - 1) == '\'') {
            return new Grammar.Terminal("CONSTANT");
        }
        // 5) 兜底：把字面转成大写终结符（适用于意外情况）
        //疑似有点丑陋了 但是能用就行
        return new Grammar.Terminal(lex.toUpperCase());
    }
}
