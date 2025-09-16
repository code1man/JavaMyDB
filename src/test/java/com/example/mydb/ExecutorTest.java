package com.example.mydb;

import org.csu.mydb.executor.ExecutionPlan;
import org.csu.mydb.executor.ExecutionResult;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.executor.ExecutorException;
import org.csu.mydb.storage.StorageEngine;
import org.csu.mydb.storage.Table.Column.Column;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Executor 模块测试类（已修正：不依赖 Column 的参数化构造函数）
 */
public class ExecutorTest {
    private Executor executor;
    private StorageEngineMock storageEngine;

    @BeforeEach
    public void setUp() {
        storageEngine = new StorageEngineMock();
        executor = new Executor(storageEngine);
    }

    @AfterEach
    public void tearDown() {
        storageEngine = null;
        executor = null;
    }

    // 辅助方法：用无参 Column + setter 快速构造 Column 对象
    private static Column makeColumn(String name, String type, int length, int scale) {
        Column c = new Column();
        c.setName(name);
        c.setType(type);
        c.setLength(length);
        c.setScale(scale);
        return c;
    }

    @Test
    @DisplayName("测试创建数据库")
    public void testCreateDatabase() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_DATABASE);
        plan.setDatabaseName("test_db");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据库创建成功: test_db", result.getMessage());
        assertTrue(storageEngine.isCreateDatabaseCalled());
    }

    @Test
    @DisplayName("测试删除数据库")
    public void testDropDatabase() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.DROP_DATABASE);
        plan.setDatabaseName("test_db");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据库删除成功: test_db", result.getMessage());
        assertTrue(storageEngine.isDropDatabaseCalled());
    }

    @Test
    @DisplayName("测试打开数据库")
    public void testOpenDatabase() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.OPEN_DATABASE);
        plan.setDatabaseName("test_db");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据库打开成功: test_db", result.getMessage());
        assertTrue(storageEngine.isOpenDatabaseCalled());
    }

    @Test
    @DisplayName("测试关闭数据库")
    public void testCloseDatabase() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CLOSE_DATABASE);

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据库关闭成功", result.getMessage());
        assertTrue(storageEngine.isCloseDatabaseCalled());
    }

    @Test
    @DisplayName("测试创建表")
    public void testCreateTable() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_TABLE);
        plan.setTableName("users");
        plan.setColumns(Arrays.asList(
                makeColumn("id", "INT", 11, 0),
                makeColumn("name", "VARCHAR", 50, 0),
                makeColumn("email", "VARCHAR", 100, 0)
        ));

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("表创建成功: users", result.getMessage());
        assertTrue(storageEngine.isCreateTableCalled());
    }

    @Test
    @DisplayName("测试删除表")
    public void testDropTable() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.DROP_TABLE);
        plan.setTableName("users");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("表删除成功: users", result.getMessage());
        assertTrue(storageEngine.isDropTableCalled());
    }

    @Test
    @DisplayName("测试插入数据")
    public void testInsert() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.INSERT);
        plan.setTableName("users");
        plan.setColumns(Arrays.asList(
                makeColumn("id", "INT", 11, 0),
                makeColumn("name", "VARCHAR", 50, 0),
                makeColumn("email", "VARCHAR", 100, 0)
        ));
        plan.setValues(Arrays.asList("1", "John Doe", "john@example.com"));

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据插入成功", result.getMessage());
        assertEquals(1, result.getAffectedRows());
        assertTrue(storageEngine.isInsertCalled());
    }

    @Test
    @DisplayName("测试删除数据")
    public void testDelete() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.DELETE);
        plan.setTableName("users");
        plan.setCondition("id=1");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据删除成功", result.getMessage());
        assertTrue(storageEngine.isDeleteCalled());
    }

    @Test
    @DisplayName("测试更新数据")
    public void testUpdate() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.UPDATE);
        plan.setTableName("users");
        plan.setSetColumn("email");
        plan.setNewValue("john.doe@example.com");
        plan.setCondition("id=1");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据更新成功", result.getMessage());
        assertTrue(storageEngine.isUpdateCalled());
    }

    @Test
    @DisplayName("测试单表查询")
    public void testQuerySingleTable() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.QUERY);
        plan.setTableName("users");
        plan.setQueryColumns("all");
        plan.setCondition("id=1");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("查询成功", result.getMessage());
        assertNotNull(result.getData());
        assertTrue(storageEngine.isQueryCalled());
    }

    @Test
    @DisplayName("测试 JOIN 查询")
    public void testQueryJoin() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.QUERY);
        plan.setTableName("users");
        plan.setJoinTableName("orders");
        plan.setJoinCondition("users.id = orders.user_id");
        plan.setQueryColumns("all");

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("查询成功", result.getMessage());
        assertNotNull(result.getData());
        assertTrue(storageEngine.isJoinQueryCalled());
    }

    @Test
    @DisplayName("测试 EXIT 命令")
    public void testExitCommand() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.EXIT);

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("退出命令已执行", result.getMessage());
    }

    @Test
    @DisplayName("测试缺少必要参数")
    public void testMissingParameters() throws ExecutorException {
        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_DATABASE);
        // 未设置 databaseName

        ExecutionResult result = executor.execute(plan);
        assertFalse(result.isSuccess());
        assertEquals("数据库名不能为空", result.getMessage());
    }
}

/**
 * 存储引擎模拟类（新版）
 */
class StorageEngineMock extends StorageEngine {
    private boolean createDatabaseCalled = false;
    private boolean dropDatabaseCalled = false;
    private boolean openDatabaseCalled = false;
    private boolean closeDatabaseCalled = false;
    private boolean createTableCalled = false;
    private boolean dropTableCalled = false;
    private boolean insertCalled = false;
    private boolean deleteCalled = false;
    private boolean updateCalled = false;
    private boolean queryCalled = false;
    private boolean joinQueryCalled = false;

    @Override
    public void myCreateDataBase(String dataBaseName) {
        createDatabaseCalled = true;
        System.out.println("模拟创建数据库: " + dataBaseName);
    }

    @Override
    public void myDropDataBase(String dataBaseName) {
        dropDatabaseCalled = true;
        System.out.println("模拟删除数据库: " + dataBaseName);
    }

    @Override
    public void myOpenDataBase(String dataBaseName) {
        openDatabaseCalled = true;
        System.out.println("模拟打开数据库: " + dataBaseName);
    }

    @Override
    public void myCloseDataBase() {
        closeDatabaseCalled = true;
        System.out.println("模拟关闭数据库");
    }

    @Override
    public void myCreateTable(String tableName, List<Column> columns) {
        createTableCalled = true;
        System.out.println("模拟创建表: " + tableName + ", 列: " + columns);
    }

    @Override
    public void myDropTable(String tableName) {
        dropTableCalled = true;
        System.out.println("模拟删除表: " + tableName);
    }

    @Override
    public void myInsert(String tableName, List<Column> columns, List<String> values) {
        insertCalled = true;
        System.out.println("模拟插入数据到表: " + tableName + ", 列: " + columns + ", 值: " + values);
    }

    @Override
    public void myDelete(String tableName, String condition) {
        deleteCalled = true;
        System.out.println("模拟删除数据: " + tableName + ", 条件: " + condition);
    }

    @Override
    public void myUpdate(String tableName, String setCol, String newValue, String condition) {
        updateCalled = true;
        System.out.println("模拟更新表: " + tableName + ", 设置列: " + setCol +
                ", 新值: " + newValue + ", 条件: " + condition);
    }

    @Override
    public void myQuery(String tableName, String columns, String condition) {
        queryCalled = true;
        System.out.println("模拟查询表: " + tableName + ", 列: " + columns + ", 条件: " + condition);
        System.out.println("id name email");
        System.out.println("1 John john@example.com");
    }

    @Override
    public void myQuery(String table1, String table2, String columns, String joinCondition, String condition) {
        joinQueryCalled = true;
        System.out.println("模拟 JOIN 查询: " + table1 + " JOIN " + table2 +
                " ON " + joinCondition + ", 列: " + columns + ", 条件: " + condition);
        System.out.println("id name order_id product");
        System.out.println("1 John 101 Book");
    }

    // getter 方法
    public boolean isCreateDatabaseCalled() { return createDatabaseCalled; }
    public boolean isDropDatabaseCalled() { return dropDatabaseCalled; }
    public boolean isOpenDatabaseCalled() { return openDatabaseCalled; }
    public boolean isCloseDatabaseCalled() { return closeDatabaseCalled; }
    public boolean isCreateTableCalled() { return createTableCalled; }
    public boolean isDropTableCalled() { return dropTableCalled; }
    public boolean isInsertCalled() { return insertCalled; }
    public boolean isDeleteCalled() { return deleteCalled; }
    public boolean isUpdateCalled() { return updateCalled; }
    public boolean isQueryCalled() { return queryCalled; }
    public boolean isJoinQueryCalled() { return joinQueryCalled; }
}
