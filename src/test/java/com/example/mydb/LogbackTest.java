package com.example.mydb;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

// LogbackTest.java
public class LogbackTest {
    private static final Logger logger = LoggerFactory.getLogger(LogbackTest.class);

    public static void main(String[] args) {
        logger.debug("Debug级别日志（应输出到控制台和文件）");
        logger.info("Info级别日志（应输出到控制台和文件）");
        logger.warn("Warn级别日志（应输出到控制台和文件）");
        logger.error("Error级别日志（应输出到控制台和文件）");
    }
}
