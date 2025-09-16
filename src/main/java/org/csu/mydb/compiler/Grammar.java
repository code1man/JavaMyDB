package org.csu.mydb.compiler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description 文法
 */
public class Grammar {
    // 非终结符
    public static class NonTerminal {
        public final String name;
        public NonTerminal(String name) { this.name = name; }
        @Override public String toString() { return name; }
        @Override public boolean equals(Object obj) {
            return obj instanceof NonTerminal && name.equals(((NonTerminal)obj).name);
        }
        @Override public int hashCode() { return name.hashCode(); }
    }

    // 终结符
    public static class Terminal {
        public final String symbol;
        public Terminal(String symbol) { this.symbol = symbol; }
        @Override public String toString() { return symbol; }
        @Override public boolean equals(Object obj) {
            return obj instanceof Terminal && symbol.equals(((Terminal)obj).symbol);
        }
        @Override public int hashCode() { return symbol.hashCode(); }
    }

    // 产生式
    public static class Production {
        public final NonTerminal left;
        public final List<Object> right; // Object = NonTerminal 或 Terminal
        public Production(NonTerminal left, List<Object> right) {
            this.left = left; this.right = right;
        }
        @Override public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(left).append(" → ");
            for (Object o : right) sb.append(o).append(" ");
            return sb.toString().trim();
        }
    }

    public final Map<NonTerminal, List<Production>> productions = new HashMap<>();
    public final Set<NonTerminal> nonTerminals = new LinkedHashSet<>();
    public final Set<Terminal> terminals = new LinkedHashSet<>();

    public static final Terminal EPSILON = new Terminal("ε");
    public Grammar() {
        // ---------------- 非终结符声明 ----------------
        NonTerminal Program = new NonTerminal("Program");
        NonTerminal StatementList = new NonTerminal("StatementList");
        NonTerminal Statement = new NonTerminal("Statement");

        NonTerminal CreateStmt = new NonTerminal("CreateStmt");
        NonTerminal CreateTail = new NonTerminal("CreateTail");

        NonTerminal CreateDBStmt = new NonTerminal("CreateDBStmt"); // 保留占位（不用直接用）
        NonTerminal CreateTableStmt = new NonTerminal("CreateTableStmt");

        NonTerminal ColumnDefList = new NonTerminal("ColumnDefList");
        NonTerminal ColumnDefListTail = new NonTerminal("ColumnDefListTail");
        NonTerminal ColumnDef = new NonTerminal("ColumnDef");
        NonTerminal TypeDef = new NonTerminal("TypeDef");
        NonTerminal ColumnConstraint = new NonTerminal("ColumnConstraint");

        NonTerminal InsertStmt = new NonTerminal("InsertStmt");
        NonTerminal ColumnList = new NonTerminal("ColumnList");
        NonTerminal ColumnListTail = new NonTerminal("ColumnListTail");
        NonTerminal ValueList = new NonTerminal("ValueList");
        NonTerminal ValueListTail = new NonTerminal("ValueListTail");
        NonTerminal Value = new NonTerminal("Value");

        NonTerminal SelectStmt = new NonTerminal("SelectStmt");
        NonTerminal WhereClause = new NonTerminal("WhereClause");
        NonTerminal Condition = new NonTerminal("Condition");
        NonTerminal ConditionTerm = new NonTerminal("ConditionTerm");
        NonTerminal ConditionTail = new NonTerminal("ConditionTail"); // 新增尾部
        NonTerminal Operator = new NonTerminal("Operator");
        NonTerminal LogicOp = new NonTerminal("LogicOp");

        NonTerminal UpdateStmt = new NonTerminal("UpdateStmt");
        NonTerminal AssignList = new NonTerminal("AssignList");
        NonTerminal Assign = new NonTerminal("Assign");

        NonTerminal DeleteStmt = new NonTerminal("DeleteStmt");
        NonTerminal DropStmt = new NonTerminal("DropStmt");
        NonTerminal UseStmt = new NonTerminal("UseStmt");

        NonTerminal AlterStmt = new NonTerminal("AlterStmt");
        NonTerminal AlterAction = new NonTerminal("AlterAction");

        NonTerminal InsertTail = new NonTerminal("InsertTail");
        NonTerminal DropTail = new NonTerminal("DropTail");
        NonTerminal AssignListTail = new NonTerminal("AssignListTail");

        NonTerminal GrantStmt = new NonTerminal("GrantStmt");
        NonTerminal PermissionList = new NonTerminal("PermissionList");
        NonTerminal PermissionListTail = new NonTerminal("PermissionListTail");
        NonTerminal Permission = new NonTerminal("Permission");
        // 注册非终结符（确保包含上面所有新声明的）
        nonTerminals.addAll(Arrays.asList(
                Program, StatementList, Statement,
                CreateStmt, CreateTail, CreateDBStmt, CreateTableStmt,
                ColumnDefList, ColumnDefListTail, ColumnDef, TypeDef, ColumnConstraint,
                InsertStmt, ColumnList, ColumnListTail, ValueList, ValueListTail, Value,
                SelectStmt, WhereClause, Condition, ConditionTerm, ConditionTail, Operator, LogicOp,
                UpdateStmt, AssignList, Assign,
                DeleteStmt, DropStmt, UseStmt,
                AlterStmt, AlterAction,
                InsertTail, DropTail, AssignListTail,
                GrantStmt, PermissionList, PermissionListTail, Permission
        ));
        // ---------------- 终结符（确保包含 EVERYTHING） ----------------
        String[] kw = {"SELECT","FROM","WHERE","CREATE","TABLE","INSERT","INTO","VALUES",
                "UPDATE","SET","DELETE","DROP","DATABASE","USE",
                "AND","NOT","PRIMARY","KEY","INT","VARCHAR","CHAR","EVERYTHING"};
        String[] symbols = {",",";","(",")","=","!=","<>",">","<",">=","<="};
        for (String s : kw) terminals.add(new Terminal(s));
        for (String s : symbols) terminals.add(new Terminal(s));
        // 基础终结符
        terminals.add(new Terminal("IDENTIFIER"));
        terminals.add(new Terminal("CONSTANT"));

        // ================== 开始产生式 ==================
        // Program -> StatementList
        addProduction(Program, Arrays.asList(StatementList));

        // StatementList -> Statement StatementList | ε
        addProduction(StatementList, Arrays.asList(Statement, StatementList));
        addProduction(StatementList, Arrays.asList(EPSILON));

        // Statement -> CreateStmt | InsertStmt | SelectStmt | UpdateStmt | DeleteStmt | DropStmt | UseStmt | AlterStmt
        addProduction(Statement, Arrays.asList(CreateStmt));
        addProduction(Statement, Arrays.asList(InsertStmt));
        addProduction(Statement, Arrays.asList(SelectStmt));
        addProduction(Statement, Arrays.asList(UpdateStmt));
        addProduction(Statement, Arrays.asList(DeleteStmt));
        addProduction(Statement, Arrays.asList(DropStmt));
        addProduction(Statement, Arrays.asList(UseStmt));
        addProduction(Statement, Arrays.asList(AlterStmt));

        //grant
       //  在 Statement 的产生式中加入 GrantStmt
                addProduction(Statement, Arrays.asList(GrantStmt));

        // 产生式：GrantStmt -> GRANT PermissionList ON IDENTIFIER TO IDENTIFIER ;
                addProduction(GrantStmt, Arrays.asList(new Terminal("GRANT"), PermissionList, new Terminal("ON"),
                        new Terminal("IDENTIFIER"), new Terminal("TO"), new Terminal("IDENTIFIER"), new Terminal(";")));

        // PermissionList -> Permission PermissionListTail
                addProduction(PermissionList, Arrays.asList(Permission, PermissionListTail));
        // PermissionListTail -> , Permission PermissionListTail | ε
                addProduction(PermissionListTail, Arrays.asList(new Terminal(","), Permission, PermissionListTail));
                addProduction(PermissionListTail, Arrays.asList(EPSILON));

        // Permission -> SELECT | UPDATE | DELETE | INSERT
                addProduction(Permission, Arrays.asList(new Terminal("SELECT")));
                addProduction(Permission, Arrays.asList(new Terminal("UPDATE")));
                addProduction(Permission, Arrays.asList(new Terminal("DELETE")));
                addProduction(Permission, Arrays.asList(new Terminal("INSERT")));
        // ---------------- Create ----------------
        // CreateStmt -> CREATE CreateTail
        addProduction(CreateStmt, Arrays.asList(new Terminal("CREATE"), CreateTail));

        // CreateTail -> DATABASE IDENTIFIER ;   (建库，示例带 ;)
        addProduction(CreateTail, Arrays.asList(new Terminal("DATABASE"), new Terminal("IDENTIFIER"), new Terminal(";")));
        // CreateTail -> TABLE IDENTIFIER ( ColumnDefList )   (建表，示例没有分号)
        // 如果你也想允许在末尾加分号，再添加一个以 ";" 结尾的产生式（注意：会有 FIRST 冲突；建议仅保留一种风格或用 OptSemicolon）
        addProduction(CreateTail, Arrays.asList(new Terminal("TABLE"), new Terminal("IDENTIFIER"), new Terminal("("), ColumnDefList, new Terminal(")")));

        // UseStmt -> USE IDENTIFIER ;
        addProduction(UseStmt, Arrays.asList(new Terminal("USE"), new Terminal("IDENTIFIER"), new Terminal(";")));

        // ---------------- Column definitions （尾部化避免冲突） ----------------
        // ColumnDefList -> ColumnDef ColumnDefListTail
        addProduction(ColumnDefList, Arrays.asList(ColumnDef, ColumnDefListTail));
        // ColumnDefListTail -> , ColumnDef ColumnDefListTail | ε
        addProduction(ColumnDefListTail, Arrays.asList(new Terminal(","), ColumnDef, ColumnDefListTail));
        addProduction(ColumnDefListTail, Arrays.asList(EPSILON));

        // ColumnDef -> IDENTIFIER TypeDef ColumnConstraint
        addProduction(ColumnDef, Arrays.asList(new Terminal("IDENTIFIER"), TypeDef, ColumnConstraint));

        // TypeDef -> INT | VARCHAR ( CONSTANT ) | CHAR ( CONSTANT )
        addProduction(TypeDef, Arrays.asList(new Terminal("INT")));
        addProduction(TypeDef, Arrays.asList(new Terminal("VARCHAR"), new Terminal("("), new Terminal("CONSTANT"), new Terminal(")")));
        addProduction(TypeDef, Arrays.asList(new Terminal("CHAR"), new Terminal("("), new Terminal("CONSTANT"), new Terminal(")")));

        // ColumnConstraint -> PRIMARY KEY | NOT NULL | ε
        addProduction(ColumnConstraint, Arrays.asList(new Terminal("PRIMARY"), new Terminal("KEY")));
        addProduction(ColumnConstraint, Arrays.asList(new Terminal("NOT"), new Terminal("NULL")));
        addProduction(ColumnConstraint, Arrays.asList(EPSILON));

        // ---------------- Insert ----------------
        // 两种形式：1) INSERT INTO id VALUES (...) ;   2) INSERT INTO id (col,...) VALUES (...) ;
        addProduction(InsertStmt, Arrays.asList(new Terminal("INSERT"), new Terminal("INTO"), new Terminal("IDENTIFIER"), InsertTail));

// InsertTail -> VALUES ( ValueList ) ;
// InsertTail -> ( ColumnList ) VALUES ( ValueList ) ;
        addProduction(InsertTail, Arrays.asList(new Terminal("VALUES"), new Terminal("("), ValueList, new Terminal(")"), new Terminal(";")));
        addProduction(InsertTail, Arrays.asList(new Terminal("("), ColumnList, new Terminal(")"), new Terminal("VALUES"), new Terminal("("), ValueList, new Terminal(")"), new Terminal(";")));
        // ValueList -> Value ValueListTail
        addProduction(ValueList, Arrays.asList(Value, ValueListTail));
        // ValueListTail -> , Value ValueListTail | ε
        addProduction(ValueListTail, Arrays.asList(new Terminal(","), Value, ValueListTail));
        addProduction(ValueListTail, Arrays.asList(EPSILON));

        // Value -> IDENTIFIER | CONSTANT
        addProduction(Value, Arrays.asList(new Terminal("IDENTIFIER")));
        addProduction(Value, Arrays.asList(new Terminal("CONSTANT")));

        // ---------------- Select ----------------
        // SelectStmt -> SELECT ColumnList FROM IDENTIFIER WhereClause ;
        addProduction(SelectStmt, Arrays.asList(new Terminal("SELECT"), ColumnList, new Terminal("FROM"), new Terminal("IDENTIFIER"), WhereClause, new Terminal(";")));

        // ColumnList -> EVERYTHING | IDENTIFIER ColumnListTail
        addProduction(ColumnList, Arrays.asList(new Terminal("EVERYTHING")));
        addProduction(ColumnList, Arrays.asList(new Terminal("IDENTIFIER"), ColumnListTail));
        // ColumnListTail -> , IDENTIFIER ColumnListTail | ε
        addProduction(ColumnListTail, Arrays.asList(new Terminal(","), new Terminal("IDENTIFIER"), ColumnListTail));
        addProduction(ColumnListTail, Arrays.asList(EPSILON));

        // WhereClause -> ε | WHERE Condition
        addProduction(WhereClause, Arrays.asList(EPSILON));
        addProduction(WhereClause, Arrays.asList(new Terminal("WHERE"), Condition));

        // Condition -> ConditionTerm ConditionTail
        addProduction(Condition, Arrays.asList(ConditionTerm, ConditionTail));
        // ConditionTail -> AND Condition | ε      (只支持 AND 链接，按你的要求)
        addProduction(ConditionTail, Arrays.asList(new Terminal("AND"), Condition));
        addProduction(ConditionTail, Arrays.asList(EPSILON));

        // ConditionTerm -> IDENTIFIER Operator Value
        addProduction(ConditionTerm, Arrays.asList(new Terminal("IDENTIFIER"), Operator, Value));

        // Operator -> = | != | <> | > | < | >= | <=
        addProduction(Operator, Arrays.asList(new Terminal("=")));
        addProduction(Operator, Arrays.asList(new Terminal("!=")));
        addProduction(Operator, Arrays.asList(new Terminal("<>")));
        addProduction(Operator, Arrays.asList(new Terminal(">")));
        addProduction(Operator, Arrays.asList(new Terminal("<")));
        addProduction(Operator, Arrays.asList(new Terminal(">=")));
        addProduction(Operator, Arrays.asList(new Terminal("<=")));

        // LogicOp is not used directly now (we used explicit "AND"), 保留以备扩展
        addProduction(LogicOp, Arrays.asList(new Terminal("AND")));

        // ---------------- Update ----------------
        // UPDATE id SET AssignList WhereClause ;
        addProduction(UpdateStmt, Arrays.asList(new Terminal("UPDATE"), new Terminal("IDENTIFIER"), new Terminal("SET"), AssignList, WhereClause, new Terminal(";")));
        // AssignList -> Assign AssignListTail
        addProduction(AssignList, Arrays.asList(Assign, AssignListTail));
// AssignListTail -> , Assign AssignListTail | ε
        addProduction(AssignListTail, Arrays.asList(new Terminal(","), Assign, AssignListTail));
        addProduction(AssignListTail, Arrays.asList(EPSILON));
        // Assign -> IDENTIFIER = Value
        addProduction(Assign, Arrays.asList(new Terminal("IDENTIFIER"), new Terminal("="), Value));

        // ---------------- Delete ----------------
        // DELETE FROM id WhereClause ;
        addProduction(DeleteStmt, Arrays.asList(new Terminal("DELETE"), new Terminal("FROM"), new Terminal("IDENTIFIER"), WhereClause, new Terminal(";")));

        // ---------------- Drop ----------------
        // ---------------- Drop（左因子化） ----------------
// 把 DropStmt 改成 Drop DropTail 的形式，避免 DropStmt 两条产生式以 DROP 为共同前缀产生冲突
// DropStmt -> DROP DropTail
        addProduction(DropStmt, Arrays.asList(new Terminal("DROP"), DropTail));
// DropTail -> TABLE IDENTIFIER ; | DATABASE IDENTIFIER ;
        addProduction(DropTail, Arrays.asList(new Terminal("TABLE"), new Terminal("IDENTIFIER"), new Terminal(";")));
        addProduction(DropTail, Arrays.asList(new Terminal("DATABASE"), new Terminal("IDENTIFIER"), new Terminal(";")));

        // ---------------- Use ----------------
        addProduction(UseStmt, Arrays.asList(new Terminal("USE"), new Terminal("IDENTIFIER"), new Terminal(";")));

        // ---------------- Alter (简单支持) ----------------
        addProduction(AlterStmt, Arrays.asList(new Terminal("ALTER"), new Terminal("TABLE"), new Terminal("IDENTIFIER"), AlterAction, new Terminal(";")));
        addProduction(AlterAction, Arrays.asList(new Terminal("ADD"), ColumnDef));
        addProduction(AlterAction, Arrays.asList(new Terminal("DROP"), new Terminal("COLUMN"), new Terminal("IDENTIFIER")));
    }

    private void addProduction(NonTerminal left, List<Object> right) {
        Production p = new Production(left, right);
        productions.computeIfAbsent(left, k -> new ArrayList<>()).add(p);
    }

    public List<Production> getProductions(NonTerminal nt) {
        return productions.getOrDefault(nt, Collections.emptyList());
    }

    // ---------- First/Follow 计算 ----------
    private final Map<NonTerminal, Set<Terminal>> firstMap = new HashMap<>();
    private final Map<NonTerminal, Set<Terminal>> followMap = new HashMap<>();

    public void computeFirstSets() {
        for (NonTerminal nt : nonTerminals) firstMap.put(nt, new HashSet<>());
        boolean changed;
        do {
            changed = false;
            for (NonTerminal nt : nonTerminals) {
                List<Production> prods = getProductions(nt);
                for (Production p : prods) {
                    boolean addEpsilon = true;
                    for (Object sym : p.right) {
                        if (sym instanceof Terminal) {
                            Terminal t = (Terminal) sym;
                            if (!t.equals(EPSILON)) {
                                if (firstMap.get(nt).add(t)) changed = true;
                            } else {
                                if (firstMap.get(nt).add(EPSILON)) changed = true;
                            }
                            addEpsilon = false;
                            break;
                        } else if (sym instanceof NonTerminal) {
                            NonTerminal B = (NonTerminal) sym;
                            Set<Terminal> fB = firstMap.get(B);
                            if (fB != null) {
                                for (Terminal t : fB) {
                                    if (!t.equals(EPSILON)) {
                                        if (firstMap.get(nt).add(t)) changed = true;
                                    }
                                }
                                if (!fB.contains(EPSILON)) {
                                    addEpsilon = false; break;
                                }
                            }
                        }
                    }
                    if (addEpsilon) {
                        if (firstMap.get(nt).add(EPSILON)) changed = true;
                    }
                }
            }
        } while (changed);
    }

    public void computeFollowSets() {
        for (NonTerminal nt : nonTerminals) followMap.put(nt, new HashSet<>());
        // 起始符号 Program 的 Follow 加 $
        followMap.get(new NonTerminal("Program")).add(new Terminal("$"));

        boolean changed;
        do {
            changed = false;
            for (NonTerminal nt : nonTerminals) {
                for (Production p : getProductions(nt)) {
                    List<Object> rhs = p.right;
                    for (int i = 0; i < rhs.size(); i++) {
                        Object sym = rhs.get(i);
                        if (sym instanceof NonTerminal) {
                            NonTerminal B = (NonTerminal) sym;
                            Set<Terminal> followB = followMap.get(B);
                            int oldSize = followB.size();

                            boolean allNullable = true;
                            for (int j = i + 1; j < rhs.size(); j++) {
                                Object beta = rhs.get(j);
                                if (beta instanceof Terminal) {
                                    Terminal t = (Terminal) beta;
                                    if (!t.equals(EPSILON)) followB.add(t);
                                    allNullable = false;
                                    break;
                                } else if (beta instanceof NonTerminal) {
                                    Set<Terminal> firstBeta = firstMap.get(beta);
                                    if (firstBeta != null) {
                                        for (Terminal t : firstBeta) {
                                            if (!t.equals(EPSILON)) followB.add(t);
                                        }
                                        if (!firstBeta.contains(EPSILON)) {
                                            allNullable = false; break;
                                        }
                                    }
                                }
                            }
                            if (i == rhs.size() - 1 || allNullable) {
                                // 将左侧的 Follow 加入 B 的 Follow
                                Set<Terminal> followLeft = followMap.get(p.left);
                                if (followLeft != null) followB.addAll(followLeft);
                            }
                            if (followB.size() > oldSize) changed = true;
                        }
                    }
                }
            }
        } while (changed);
    }
    private static final Logger logger = LoggerFactory.getLogger(Grammar.class);

    public void printFirstSets() {
       logger.info("=== FIRST 集 ===");
        for (NonTerminal nt : nonTerminals) {
            logger.info(nt + ": { ");
            Set<Terminal> s = firstMap.get(nt);
            if (s != null) for (Terminal t : s)  logger.info(t + " ");
            logger.info("}");
        }
    }

    public void printFollowSets() {
        logger.info("=== FOLLOW 集 ===");
        for (NonTerminal nt : nonTerminals) {
            logger.info(nt + ": { ");
            Set<Terminal> s = followMap.get(nt);
            if (s != null) for (Terminal t : s)  logger.info(t + " ");
            logger.info("}");
        }
    }

    public Set<Terminal> getFirst(NonTerminal nt) { return firstMap.get(nt); }
    public Set<Terminal> getFollow(NonTerminal nt) { return followMap.get(nt); }
}