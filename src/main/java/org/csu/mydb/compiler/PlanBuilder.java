package org.csu.mydb.compiler;

import org.csu.mydb.executor.ExecutionPlan;
import org.csu.mydb.storage.Table.Column.Column;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.*;
/**
 * @author ljyljy
 * @date 2025/9/12
 * @description 将合法语句转换为执行计划对象
 */
public class PlanBuilder {
    /**
     * PlanBuilder: 从 token 列表构建 ExecutionPlan（支持多条语句）
     *
     * 依赖：ExecutionPlan 类存在并提供相应 setter。
     */
        private List<Lexer.Token> tokens;
        private int idx;

        public PlanBuilder() {}

        // 高层接口：把整个 token 列表 -> 多个 ExecutionPlan
        public List<ExecutionPlan> buildAll(List<Lexer.Token> tokens) throws SemanticException {
            this.tokens = tokens;
            this.idx = 0;
            List<ExecutionPlan> plans = new ArrayList<>();

            skipOptionalSemicolons();
            while (idx < tokens.size()) {
                ExecutionPlan plan = parseStatement();
                if (plan != null) plans.add(plan);
                skipOptionalSemicolons();
            }
            return plans;
        }

        // ---------- 基础工具方法 ----------
        private Lexer.Token peek() {
            if (idx >= tokens.size()) return null;
            return tokens.get(idx);
        }
        private Lexer.Token consume() {
            Lexer.Token t = peek();
            if (t != null) idx++;
            return t;
        }
        private boolean acceptKeyword(String kw) {
            Lexer.Token t = peek();
            if (t != null && t.type == Lexer.KEYWORD && t.lexeme.equalsIgnoreCase(kw)) {
                consume(); return true;
            }
            return false;
        }
        private boolean acceptDelimiter(String d) {
            Lexer.Token t = peek();
            if (t != null && t.type == Lexer.DELIMITER && t.lexeme.equals(d)) {
                consume(); return true;
            }
            return false;
        }
        private boolean acceptOperator(String op) {
            Lexer.Token t = peek();
            if (t != null && t.type == Lexer.OPERATOR && t.lexeme.equals(op)) {
                consume(); return true;
            }
            return false;
        }
    private void expectOperator(String kw) throws SemanticException {
        if (!acceptOperator(kw)) {
            Lexer.Token t = peek();
            throw error("期望操作符 '" + kw + "'", t);
        }
    }
        private void expectKeyword(String kw) throws SemanticException {
            if (!acceptKeyword(kw)) {
                Lexer.Token t = peek();
                throw error("期望关键字 '" + kw + "'", t);
            }
        }
        private void expectDelimiter(String d) throws SemanticException {
            if (!acceptDelimiter(d)) {
                Lexer.Token t = peek();
                throw error("期望 界符 '" + d + "'", t);
            }
        }
        private void skipOptionalSemicolons() {
            while (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(";")) consume();
        }
        private SemanticException error(String msg, Lexer.Token at) {
            if (at == null) return new SemanticException(msg + " (在文件末尾)");
            return new SemanticException(msg + " 在 " + at.line + ":" + at.column + " (token=" + at.lexeme + ")");
        }

        // 将 token 转成表示名（便于判断）
        private String tkName(Lexer.Token tk) {
            if (tk == null) return null;
            if (tk.type == Lexer.KEYWORD) return tk.lexeme.toUpperCase();
            if (tk.type == Lexer.IDENTIFIER) return "IDENTIFIER";
            if (tk.type == Lexer.CONSTANT) return "CONSTANT";
            if (tk.type == Lexer.OPERATOR) return tk.lexeme; // = != ...
            if (tk.type == Lexer.DELIMITER) return tk.lexeme; // ( , ) ;
            return tk.lexeme;
        }

        // ---------- 语句分发 ----------
        private ExecutionPlan parseStatement() throws SemanticException {
            Lexer.Token t = peek();
            if (t == null) return null;
            if (t.type == Lexer.KEYWORD) {
                String kw = t.lexeme.toUpperCase();
                switch (kw) {
                    case "CREATE": return parseCreate();
                    case "INSERT": return parseInsert();
                    case "SELECT": return parseSelect();
                    case "UPDATE": return parseUpdate();
                    case "DELETE": return parseDelete();
                    case "DROP":   return parseDrop();
                    case "USE":    return parseUse();
                    case "ALTER":  return parseAlter(); // 简单支持
                    case "GRANT": return parseGrant();
                    default:
                        throw error("不支持的顶层关键字：" + kw, t);
                }
            } else {
                throw error("语句必须以关键字开始", t);
            }
        }

        // ---------- CREATE (DATABASE | TABLE) ----------
        private ExecutionPlan parseCreate() throws SemanticException {
            expectKeyword("CREATE");
            Lexer.Token next = peek();
            if (next == null) throw error("CREATE 之后缺少内容", next);

            if (next.type == Lexer.KEYWORD && next.lexeme.equalsIgnoreCase("DATABASE")) {
                // CREATE DATABASE db;
                consume(); // DATABASE
                Lexer.Token name = peek();
                if (name == null || name.type != Lexer.IDENTIFIER) throw error("期望数据库名", name);
                String dbName = name.lexeme; consume();
                // 可选分号
                if (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(";")) consume();

                ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_DATABASE);
                p.setDatabaseName(dbName);
                return p;
            } else if (next.type == Lexer.KEYWORD && next.lexeme.equalsIgnoreCase("TABLE")) {
                // CREATE TABLE t ( col ... )
                consume(); // TABLE
                Lexer.Token tname = peek();
                if (tname == null || tname.type != Lexer.IDENTIFIER) throw error("期望表名", tname);
                String tableName = tname.lexeme; consume();
                expectDelimiter("(");
                List<String> cols = parseColumnDefList();
                expectDelimiter(")");
                // Create table in your examples sometimes lacks semicolon; accept optional ;
                if (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(";")) consume();

                ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_TABLE);
                p.setTableName(tableName);

                List<Column> columns = new ArrayList<>();
                for (int i = 0; i < cols.size(); i++) {
                    columns.add(Column.parseColumn(cols.get(i), i)) ;
                }
                p.setColumns(columns);
                return p;
            } else {
                throw error("CREATE 后面必须跟 DATABASE 或 TABLE", next);
            }
        }

        // ColumnDefList -> ColumnDef (',' ColumnDef)*
        private List<String> parseColumnDefList() throws SemanticException {
            List<String> cols = new ArrayList<>();
            cols.add(parseSingleColumnDef());
            while (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(",")) {
                consume(); // ,
                cols.add(parseSingleColumnDef());
            }
            return cols;
        }

        // ColumnDef -> IDENTIFIER TypeDef ColumnConstraint
        private String parseSingleColumnDef() throws SemanticException {
            Lexer.Token name = peek();
            if (name == null || name.type != Lexer.IDENTIFIER) throw error("期望列名", name);
            String colName = name.lexeme; consume();

            // TypeDef
            Lexer.Token tk = peek();
            if (tk == null || tk.type != Lexer.KEYWORD) throw error("期望类型 (INT | VARCHAR(...))", tk);
            String typeKw = tk.lexeme.toUpperCase(); consume();
            String typeStr;
            if ("INT".equals(typeKw)) {
                typeStr = "INT";
            } else if ("VARCHAR".equals(typeKw) || "CHAR".equals(typeKw)) {
                // expect ( CONSTANT )
                expectDelimiter("(");
                Lexer.Token lenTk = peek();
                if (lenTk == null || lenTk.type != Lexer.CONSTANT) throw error("期望常量作为类型长度", lenTk);
                String len = lenTk.lexeme; consume();
                expectDelimiter(")");
                typeStr = typeKw + "(" + len + ")";
            } else {
                throw error("不支持的类型: " + typeKw, tk);
            }

            // ColumnConstraint (PRIMARY KEY | NOT NULL | ε)
            String constraint = "";
            Lexer.Token maybe = peek();
            if (maybe != null && maybe.type == Lexer.KEYWORD) {
                String up = maybe.lexeme.toUpperCase();
                if ("PRIMARY".equals(up)) {
                    consume(); // PRIMARY
                    expectKeyword("KEY");
                    constraint = "PRIMARY KEY";
                } else if ("NOT".equals(up)) {
                    consume(); // NOT
                    expectKeyword("NULL");
                    constraint = "NOT NULL";
                }
            }

            String colDesc = colName + " " + typeStr + (constraint.isEmpty() ? "" : " " + constraint);
            return colDesc;
        }

        // ---------- INSERT ----------
        private ExecutionPlan parseInsert() throws SemanticException {
            expectKeyword("INSERT");
            expectKeyword("INTO");
            Lexer.Token tbl = peek();
            if (tbl == null || tbl.type != Lexer.IDENTIFIER) throw error("期望表名", tbl);
            String tableName = tbl.lexeme; consume();

            // Lookahead: either VALUES or '(' for column list
            Lexer.Token next = peek();
            if (next == null) throw error("INSERT 后缺少内容", next);

            ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.INSERT);
            p.setTableName(tableName);

            if (next.type == Lexer.KEYWORD && next.lexeme.equalsIgnoreCase("VALUES")) {
                consume(); // VALUES
                expectDelimiter("(");
                List<String> vals = parseValueList();

                expectDelimiter(")");
                expectDelimiter(";"); // insert in our grammar uses ;
                p.setValues(vals);
                return p;
            } else if (next.type == Lexer.DELIMITER && next.lexeme.equals("(")) {
                // ( col, ... ) VALUES ( val, ... ) ;
                consume(); // (
                List<String> cols = parseColumnList();
                expectDelimiter(")");
                expectKeyword("VALUES");
                expectDelimiter("(");
                List<String> vals = parseValueList();
                expectDelimiter(")");
                expectDelimiter(";");

                p.setColumns(generateColumns(vals, cols));
                p.setValues(vals);

                return p;
            } else {
                throw error("INSERT 语法错误，期望 VALUES 或列列表", next);
            }
        }

        private List<String> parseValueList() throws SemanticException {
            List<String> vals = new ArrayList<>();
            Lexer.Token v = peek();
            if (v == null) throw error("期望值", v);
            // allow IDENTIFIER or CONSTANT
                //因为没有判断字符串这种类型 导致需要自己手动设置type为constant 否则识别失败
            String lex = v.lexeme;

            if (lex != null && lex.length() >= 2 && lex.charAt(0) == '\'' && lex.charAt(lex.length() - 1) == '\'') {
                lex = lex.substring(1, lex.length() - 1);
                vals.add("#" + lex); consume();
            }
            else if (v.type == Lexer.IDENTIFIER || v.type == Lexer.CONSTANT) {
                vals.add(v.lexeme); consume();
            } else {
                throw error("值应为 IDENTIFIER 或 CONSTANT", v);
            }
            while (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(",")) {
                consume(); // ,
                Lexer.Token v2 = peek();
                if (v2 == null) throw error("期望值", v2);
                String lex1 = v2.lexeme;
                 if (lex1 != null && lex1.length() >= 2 && lex1.charAt(0) == '\'' && lex1.charAt(lex1.length() - 1) == '\'') {
                    lex1 = lex1.substring(1, lex1.length() - 1);
                    vals.add("#" + lex1); consume();
                }
               else if (v2.type == Lexer.IDENTIFIER || v2.type == Lexer.CONSTANT) {
                    vals.add(v2.lexeme); consume();
                } else {
                    throw error("值应为 IDENTIFIER 或 CONSTANT", v2);
                }
            }
            return vals;
        }

        private List<String> parseColumnList() throws SemanticException {
            List<String> cols = new ArrayList<>();
            Lexer.Token c = peek();
            if (c == null || c.type != Lexer.IDENTIFIER) throw error("期望列名", c);
            cols.add(c.lexeme); consume();
            while (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(",")) {
                consume(); // ,
                Lexer.Token c2 = peek();
                if (c2 == null || c2.type != Lexer.IDENTIFIER) throw error("期望列名", c2);
                cols.add(c2.lexeme); consume();
            }
            return cols;
        }

        // ---------- SELECT ----------
        private ExecutionPlan parseSelect() throws SemanticException {
            expectKeyword("SELECT");
            ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.QUERY);
            // ColumnList: either EVERYTHING or identifiers
            Lexer.Token first = peek();
            if (first != null && first.type == Lexer.KEYWORD && first.lexeme.equalsIgnoreCase("EVERYTHING")) {
                consume();
                p.setQueryColumns("*");
            }else {
                List<String> cols = new ArrayList<>();
                cols.add(parseQualifiedName()); // parse first qualified name
                while (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(",")) {
                    consume();
                    cols.add(parseQualifiedName());
                }
                p.setQueryColumns(String.join(",", cols));
            }

            // 2) FROM tableRef (with optional alias)
            expectKeyword("FROM");
            TableRef left = parseTableRef();
            p.setTableName(left.name);
            if (left.alias != null) p.setTableAlias(left.alias);

            // 3) optional JOIN ... ON ...
            if (peek() != null && peek().type == Lexer.KEYWORD && peek().lexeme.equalsIgnoreCase("JOIN")) {
                consume(); // JOIN
                TableRef right = parseTableRef();
                p.setJoinTableName(right.name);
                if (right.alias != null) p.setJoinTableAlias(right.alias);

                // ON
                expectKeyword("ON");
                String leftQual = parseQualifiedName();  // e.g., A.col
                // operator should be '='
                Lexer.Token op = peek();
                if (op == null || !( (op.type == Lexer.OPERATOR && op.lexeme.equals("=")) || (op.type==Lexer.DELIMITER && op.lexeme.equals("=")) )) {
                    throw error("期望 '=' 在 JOIN ON", op);
                }
                consume();
                String rightQual = parseQualifiedName();

                // store join condition as string "A.col = B.col"
                p.setJoinCondition(leftQual + " = " + rightQual);
            }

//            } else {
//                List<String> cols = parseColumnList();
//                p.setQueryColumns(String.join(",", cols));
//            }
//            expectKeyword("FROM");
//            Lexer.Token tbl = peek();
//            if (tbl == null || tbl.type != Lexer.IDENTIFIER) throw error("期望表名", tbl);
//            p.setTableName(tbl.lexeme); consume();

            // optional WHERE
            if (peek() != null && peek().type == Lexer.KEYWORD && peek().lexeme.equalsIgnoreCase("WHERE")) {
                consume(); // WHERE
                String cond = parseCondition();
                p.setCondition(cond);
            } else {
                p.setCondition(null);
            }
            // expect semicolon
            expectDelimiter(";");
           // p.setOperationType();
            return p;
        }
    // Helper small holder for table/alias
    private static class TableRef {
        String name;
        String alias;
        TableRef(String n, String a) { this.name = n; this.alias = a; }
    }

    // parse TableRef -> IDENTIFIER ( IDENTIFIER )?
    private TableRef parseTableRef() throws SemanticException {
        Lexer.Token t = peek();
        if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望表名", t);
        String name = t.lexeme; consume();
        String alias = null;
        // optional alias: an IDENTIFIER immediately following (but not a keyword like JOIN/ON)
        Lexer.Token next = peek();
        if (next != null && next.type == Lexer.IDENTIFIER) {
            // if it's an identifier and not a SQL keyword, treat as alias
            alias = next.lexeme; consume();
        }
        return new TableRef(name, alias);
    }

    // parse QualifiedName -> IDENTIFIER ( '.' IDENTIFIER )?
    private String parseQualifiedName() throws SemanticException {
        Lexer.Token t = peek();
        if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望标识符/限定名", t);
        String left = t.lexeme;
        consume();
        if (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(".")) {
            consume(); // dot
            Lexer.Token right = peek();
            if (right == null || right.type != Lexer.IDENTIFIER) throw error("期望列名在 '.' 之后", right);
            String col = right.lexeme;
            consume();
            return left + "." + col;
        } else {
            return left;
        }
    }
        // Condition -> ConditionTerm ( AND Condition )?
        // We'll reconstruct a textual condition like "a = 3 AND b = 'x'"
        private String parseCondition () throws SemanticException {
            StringBuilder sb = new StringBuilder();
            sb.append(parseConditionTerm());
            while (peek() != null && peek().type == Lexer.KEYWORD && peek().lexeme.equalsIgnoreCase("AND")) {
                consume(); // AND
                sb.append(" AND ");
                sb.append(parseConditionTerm());
            }
            return sb.toString();
        }

        private String parseConditionTerm () throws SemanticException {
            Lexer.Token col = peek();
            if (col == null || col.type != Lexer.IDENTIFIER) throw error("期望列名", col);
            String left = col.lexeme;
            consume();

            Lexer.Token op = peek();
            if (op == null || op.type != Lexer.OPERATOR && !(op.type == Lexer.DELIMITER && (op.lexeme.equals("=")))) {
                // some Lexers classify '=' as OPERATOR; defensive check included
                throw error("期望比较运算符", op);
            }
            String oper = op.lexeme;
            consume();

            Lexer.Token val = peek();
            if (val == null) throw error("期望比较值", val);
            //因为没有判断字符串这种类型 导致需要自己手动设置type为constant 否则识别失败
            String lex = val.lexeme;
            if (lex != null && lex.length() >= 2 && lex.charAt(0) == '\'' && lex.charAt(lex.length() - 1) == '\'') {
                lex = lex.substring(1, lex.length() - 1);
                String rval = lex;
                consume();
                return left + " " + oper + " " + rval;
            }
            if (val.type == Lexer.IDENTIFIER || val.type == Lexer.CONSTANT) {
                String rval = val.lexeme;
                consume();
                return left + " " + oper + " " + rval;
            } else {
                throw error("比较值应为 IDENTIFIER 或 CONSTANT", val);
            }
        }
        // ---------- UPDATE ----------
        private ExecutionPlan parseUpdate() throws SemanticException {
            expectKeyword("UPDATE");
            Lexer.Token t = peek();
            if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望表名", t);
            String tableName = t.lexeme; consume();
            expectKeyword("SET");

            // Parse AssignList but we only support single column modification as your Executor expects single setColumn/newValue
            // Accept "col = val" possibly followed by ", ..." but we'll keep only the first assignment.
            Lexer.Token a1 = peek();
            if (a1 == null || a1.type != Lexer.IDENTIFIER) throw error("期望赋值列名", a1);
            String setCol = a1.lexeme; consume();
            expectOperator("="); // '=' may be DELIMITER or OPERATOR in Lexer; if your Lexer uses OPERATOR for "=", alter accept above
            Lexer.Token newVal = peek();
            if (newVal == null || (newVal.type != Lexer.CONSTANT && newVal.type != Lexer.IDENTIFIER)) throw error("期望新值", newVal);
            String newValue = newVal.lexeme; consume();

            // skip possible additional assignments separated by comma (we ignore them or you can error)
            while (peek() != null && peek().type == Lexer.DELIMITER && peek().lexeme.equals(",")) {
                // consume sequence ", IDENTIFIER = value"
                consume();
                // consume column name
                if (peek() != null && peek().type == Lexer.IDENTIFIER) consume();
                // accept '=' then value
                if (peek() != null && (peek().type == Lexer.DELIMITER && peek().lexeme.equals("=") || peek().type == Lexer.OPERATOR && peek().lexeme.equals("="))) consume();
                if (peek() != null && (peek().type == Lexer.CONSTANT || peek().type == Lexer.IDENTIFIER)) consume();
            }

            // WHERE condition (required by your executor)
            expectKeyword("WHERE");
            String cond = parseCondition();
            expectDelimiter(";");

            ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.UPDATE);
            p.setTableName(tableName);
            p.setSetColumn(setCol);
            p.setNewValue(newValue);
            p.setCondition(cond);
            return p;
        }

        // ---------- DELETE ----------
        private ExecutionPlan parseDelete() throws SemanticException {
            expectKeyword("DELETE");
            expectKeyword("FROM");
            Lexer.Token t = peek();
            if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望表名", t);
            String tableName = t.lexeme; consume();

            expectKeyword("WHERE");
            String cond = parseCondition();
            expectDelimiter(";");

            ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.DELETE);
            p.setTableName(tableName);
            p.setCondition(cond);
            return p;
        }

        // ---------- DROP ----------
        private ExecutionPlan parseDrop() throws SemanticException {
            expectKeyword("DROP");
            Lexer.Token next = peek();
            if (next == null) throw error("DROP 后缺少内容", next);
            if (next.type == Lexer.KEYWORD && next.lexeme.equalsIgnoreCase("TABLE")) {
                consume(); // TABLE
                Lexer.Token t = peek();
                if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望表名", t);
                String tableName = t.lexeme; consume();
                expectDelimiter(";");
                ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.DROP_TABLE);
                p.setTableName(tableName);
                return p;
            } else if (next.type == Lexer.KEYWORD && next.lexeme.equalsIgnoreCase("DATABASE")) {
                consume(); // DATABASE
                Lexer.Token t = peek();
                if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望数据库名", t);
                String dbName = t.lexeme; consume();
                expectDelimiter(";");
                ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.DROP_DATABASE);
                p.setDatabaseName(dbName);
                return p;
            } else {
                throw error("DROP 后应跟 TABLE 或 DATABASE", next);
            }
        }

        // ---------- USE ----------
        private ExecutionPlan parseUse() throws SemanticException {
            expectKeyword("USE");
            Lexer.Token t = peek();
            if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望数据库名", t);
            String db = t.lexeme; consume();
            expectDelimiter(";");
            ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.OPEN_DATABASE);
            p.setDatabaseName(db);
            return p;
        }
    // 假设在 PlanBuilder/Parser 类内，返回 ExecutionPlan
    private ExecutionPlan parseGrant() throws SemanticException {
        // 入口：当前 token 是 GRANT
        expectKeyword("GRANT");

        // 1. 解析权限列表
        List<String> perms = new ArrayList<>();
        Lexer.Token tk = peek();
        if (tk == null) throw error("期望权限名称", tk);

        while (true) {
            Lexer.Token p = peek();
            if (p == null) throw error("期望权限 (SELECT/UPDATE/DELETE/INSERT)", p);
            if (p.type != Lexer.KEYWORD) throw error("权限应为关键字 (SELECT/UPDATE/DELETE/INSERT)", p);
            String up = p.lexeme.toUpperCase();
            if (!("SELECT".equals(up) || "UPDATE".equals(up) || "DELETE".equals(up) || "INSERT".equals(up))) {
                throw error("不支持的权限: " + up, p);
            }
            perms.add(up);
            consume();

            Lexer.Token next = peek();
            if (next != null && next.type == Lexer.DELIMITER && ",".equals(next.lexeme)) {
                consume(); // 吞掉逗号，继续
                continue;
            } else {
                break;
            }
        }

        // 2. ON db
        expectKeyword("ON");
        Lexer.Token dbToken = peek();
        if (dbToken == null || dbToken.type != Lexer.IDENTIFIER) throw error("期望数据库名", dbToken);
        String dbName = dbToken.lexeme;
        consume();

        // 3. TO username
        expectKeyword("TO");
        Lexer.Token userToken = peek();
        if (userToken == null || userToken.type != Lexer.IDENTIFIER) throw error("期望用户名", userToken);
        String userName = userToken.lexeme;
        consume();

        // 4. 结尾 ;
        expectDelimiter(";");

        // 构造执行计划
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.GRANT);
        plan.setOperationType(ExecutionPlan.OperationType.GRANT);
        plan.setDatabaseName(dbName);
        plan.setGrants(perms);
        plan.setGrantee(userName);

        return plan;
    }

        // ---------- ALTER (简单版) ----------
        private ExecutionPlan parseAlter() throws SemanticException {
            // 支持 ALTER TABLE name ADD columnDef ;  或 ALTER TABLE name DROP COLUMN col ;
            expectKeyword("ALTER");
            expectKeyword("TABLE");
            Lexer.Token t = peek();
            if (t == null || t.type != Lexer.IDENTIFIER) throw error("期望表名", t);
            String tableName = t.lexeme; consume();
            Lexer.Token action = peek();
            if (action == null || action.type != Lexer.KEYWORD) throw error("期望 ADD 或 DROP", action);

            ExecutionPlan p = new ExecutionPlan(ExecutionPlan.OperationType.UPDATE);
            p.setTableName(tableName);

            List<Column> columns = new ArrayList<>();
            if (action.lexeme.equalsIgnoreCase("ADD")) {
                consume(); // ADD
                String colDef = parseSingleColumnDef();
                // 你可以把这个信息放到 columns 字段里做新增

                // TODO: column
                // p.setColumns(colDef);
            } else if (action.lexeme.equalsIgnoreCase("DROP")) {
                consume(); // DROP
                expectKeyword("COLUMN");
                Lexer.Token col = peek();
                if (col == null || col.type != Lexer.IDENTIFIER) throw error("期望列名", col);

                // TODO
                // p.setColumns(col.lexeme);
                consume();
            } else {
                throw error("不支持的 ALTER 操作", action);
            }
            expectDelimiter(";");
            return p;
        }

        // ---------- Exception ----------
        public static class SemanticException extends Exception {
            public SemanticException(String msg) { super(msg); }
        }

        // ------------UTIL--------------
        private List<Column> generateColumns(List<String> vals, List<String> cols){
            List<Column> columns = new ArrayList<>();

            // 构造Column
            for (int i = 0; i < vals.size(); i++) {
                Column column = new Column();

                // TYPE
                if (vals.get(i).charAt(0) == '#') {
                    column.setType("VARCHAR");
                } else {
                    column.setType("INT");
                }

                // NAME
                column.setName(cols.get(i));

                // POS
                column.setPosition(i);

                columns.add(column);
            }
            return columns;
        }
    }

