package com.example.mydb;

import org.csu.mydb.config.ConfigManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ConfigManagerTest {
    // 测试前加载配置文件（仅执行一次）
    @BeforeAll
    public static void setup() throws Exception {
        ConfigManager.getInstance().loadConfig("config/mydb_test.ini");
    }

    // 测试 1：配置文件加载是否成功（无异常即成功）
    @Test
    public void testLoadConfig_Success() {
        assertDoesNotThrow(() -> ConfigManager.getInstance().loadConfig("config/mydb_test.ini"));
    }

    // 测试 2：获取存在的字符串配置项
    @Test
    public void testGetString_ExistingKey() {
        String logLevel = ConfigManager.getInstance().getString("log", "level", "INFO");
        assertEquals("DEBUG", logLevel);  // 验证正确获取
    }

    // 测试 3：获取不存在的字符串配置项（使用默认值）
    @Test
    public void testGetString_MissingKey() {
        String nonExistentKey = ConfigManager.getInstance().getString("log", "non_existent", "DEFAULT");
        assertEquals("DEFAULT", nonExistentKey);  // 验证返回默认值
    }

    // 测试 4：获取存在的整数配置项
    @Test
    public void testGetInt_ExistingKey() {
        int pageSize = ConfigManager.getInstance().getInt("storage", "page_size", 4096);
        assertEquals(8192, pageSize);  // 验证正确转换
    }

    // 测试 5：获取格式错误的整数配置项（返回默认值）
    @Test
    public void testGetInt_InvalidFormat() {
        int invalidInt = ConfigManager.getInstance().getInt("storage", "invalid_int", 4096);
        assertEquals(4096, invalidInt);  // 验证返回默认值并输出警告
    }

    // 测试 6：获取存在的布尔配置项
    @Test
    public void testGetBool_ExistingKey() {
        boolean cacheEnabled = ConfigManager.getInstance().getBool("log", "enabled", false);
        assertTrue(cacheEnabled);  // 验证正确转换
    }

    // 测试 7：获取格式错误的布尔配置项（返回默认值）
    @Test
    public void testGetBool_InvalidFormat() {
        boolean invalidBool = ConfigManager.getInstance().getBool("log", "path", false);  // path 是字符串，非布尔
        assertFalse(invalidBool);  // 验证返回默认值并输出警告
    }

    // 测试 8：获取缺失节中的配置项（返回默认值）
    @Test
    public void testGetString_MissingSection() {
        String missingSectionKey = ConfigManager.getInstance().getString("missing_section", "any_key", "DEFAULT");
        assertEquals("DEFAULT", missingSectionKey);  // 验证返回默认值
    }

    // 测试 9：获取空值键的配置项（返回默认值）
    @Test
    public void testGetString_EmptyValue() {
        String emptyValueKey = ConfigManager.getInstance().getString("existing_section", "missing_key", "DEFAULT");
        assertEquals("DEFAULT", emptyValueKey);  // 验证返回默认值
    }

    // 测试 10：多线程环境下配置获取的线程安全（可选）
    @Test
    public void testThreadSafety() throws InterruptedException {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        // 创建多个线程并发获取配置项
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100; j++) {
                    String value = ConfigManager.getInstance().getString("log", "level", "INFO");
                    assertEquals("DEBUG", value);  // 验证每次获取结果一致
                }
            });
        }

        // 启动所有线程并等待完成
        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
