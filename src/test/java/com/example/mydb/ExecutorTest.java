package com.example.mydb;

import org.csu.mydb.executor.ExecutionPlan;
import org.csu.mydb.executor.ExecutionResult;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.executor.ExecutorException;
import org.csu.mydb.storage.StorageEngine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;

/**
 * Executor模块测试类
 */
public class ExecutorTest {
    private static final Logger logger = LoggerFactory.getLogger(ExecutorTest.class);
    private Executor executor;
    private StorageEngineMock storageEngine;

    @BeforeEach
    public void setUp() {
        logger.info("初始化测试环境");
        storageEngine = new StorageEngineMock();
        executor = new Executor(storageEngine);
    }

    @AfterEach
    public void tearDown() {
        logger.info("清理测试环境");
    }

    @Test
    @DisplayName("测试创建数据库")
    public void testCreateDatabase() {
        logger.info("测试创建数据库");

        try {
            ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_DATABASE);
            plan.setDatabaseName("test_db");

            ExecutionResult result = executor.execute(plan);
            assertTrue(result.isSuccess());
            assertEquals("数据库创建成功: test_db", result.getMessage());
            assertTrue(storageEngine.isCreateDatabaseCalled());
        } catch (ExecutorException e) {
            fail("执行计划失败: " + e.getMessage());
        }
    }
    @Test
    @DisplayName("测试删除数据库")
    public void testDropDatabase() throws ExecutorException {
        logger.info("测试删除数据库");

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
        logger.info("测试打开数据库");

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
        logger.info("测试关闭数据库");

        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CLOSE_DATABASE);

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("数据库关闭成功", result.getMessage());
        assertTrue(storageEngine.isCloseDatabaseCalled());
    }

    @Test
    @DisplayName("测试创建表")
    public void testCreateTable() throws ExecutorException {
        logger.info("测试创建表");

        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_TABLE);
        plan.setTableName("users");
        plan.setColumns(Arrays.asList("id", "name", "email"));

        ExecutionResult result = executor.execute(plan);
        assertTrue(result.isSuccess());
        assertEquals("表创建成功: users", result.getMessage());
        assertTrue(storageEngine.isCreateTableCalled());
    }

    @Test
    @DisplayName("测试删除表")
    public void testDropTable() throws ExecutorException {
        logger.info("测试删除表");

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
        logger.info("测试插入数据");

        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.INSERT);
        plan.setTableName("users");
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
        logger.info("测试删除数据");

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
        logger.info("测试更新数据");

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
    @DisplayName("测试查询数据")
    public void testQuery() throws ExecutorException {
        logger.info("测试查询数据");

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
    @DisplayName("测试缺少必要参数")
    public void testMissingParameters() throws ExecutorException {
        logger.info("测试缺少必要参数");

        ExecutionPlan plan = new ExecutionPlan(ExecutionPlan.OperationType.CREATE_DATABASE);
        // 不设置databaseName

        ExecutionResult result = executor.execute(plan);
        assertFalse(result.isSuccess());
        assertEquals("数据库名不能为空", result.getMessage());
    }
}


/**
 * 存储引擎模拟类，用于测试
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
    public void myCreateTable(String tableName, List<String> columns) {
        createTableCalled = true;
        System.out.println("模拟创建表: " + tableName + ", 列: " + columns);
    }

    @Override
    public void myDropTable(String tableName) {
        dropTableCalled = true;
        System.out.println("模拟删除表: " + tableName);
    }

    @Override
    public void myInsert(String tableName, List<String> values) {
        insertCalled = true;
        System.out.println("模拟插入数据到表: " + tableName + ", 值: " + values);
    }

    @Override
    public void myDelete(String tableName, String condition) {
        deleteCalled = true;
        System.out.println("模拟从表删除数据: " + tableName + ", 条件: " + condition);
    }

    @Override
    public void myUpdate(String tableName, String setCol, String newValue, String condition) {
        updateCalled = true;
        System.out.println("模拟更新表数据: " + tableName + ", 设置列: " + setCol +
                ", 新值: " + newValue + ", 条件: " + condition);
    }

    @Override
    public void myQuery(String tableName, String columns, String condition) {
        queryCalled = true;
        System.out.println("模拟查询表: " + tableName + ", 列: " + columns + ", 条件: " + condition);
        // 模拟输出一些数据
        System.out.println("id name email");
        System.out.println("1 John john@example.com");
        System.out.println("2 Jane jane@example.com");
    }

    // 检查方法是否被调用的getter方法
    public boolean isCreateDatabaseCalled() {
        return createDatabaseCalled;
    }

    public boolean isDropDatabaseCalled() {
        return dropDatabaseCalled;
    }

    public boolean isOpenDatabaseCalled() {
        return openDatabaseCalled;
    }

    public boolean isCloseDatabaseCalled() {
        return closeDatabaseCalled;
    }

    public boolean isCreateTableCalled() {
        return createTableCalled;
    }

    public boolean isDropTableCalled() {
        return dropTableCalled;
    }

    public boolean isInsertCalled() {
        return insertCalled;
    }

    public boolean isDeleteCalled() {
        return deleteCalled;
    }

    public boolean isUpdateCalled() {
        return updateCalled;
    }

    public boolean isQueryCalled() {
        return queryCalled;
    }
}
