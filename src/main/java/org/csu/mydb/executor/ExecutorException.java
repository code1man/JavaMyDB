package org.csu.mydb.executor;



/**
 * 执行器异常类
 */
public class ExecutorException extends Exception {
    public ExecutorException(String message) {
        super(message);
    }

    public ExecutorException(String message, Throwable cause) {
        super(message, cause);
    }
}