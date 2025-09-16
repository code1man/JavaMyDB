package com.example.mydb;

import org.csu.mydb.compiler.Lexer;
import org.csu.mydb.compiler.PlanBuilder;
import org.csu.mydb.executor.ExecutionPlan;
import org.csu.mydb.executor.ExecutionResult;
import org.csu.mydb.executor.Executor;
import org.csu.mydb.executor.ExecutorException;

import java.util.List;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description PlanBuilderTest
 */
public class PlanBuilderTest {
    // 1) 从 lexer 获取 tokens
    public static void main(String[] args) throws PlanBuilder.SemanticException, ExecutorException {
        List<Lexer.Token> tokens = Lexer.tokenize("GRANT SELECT,update ON testdb TO user1;");

        // 2) 构建 plan
        PlanBuilder pb = new PlanBuilder();
        List<ExecutionPlan> plans = pb.buildAll(tokens);

        // 3) 给 Executor 执行
        Executor exec = new Executor();

        for (ExecutionPlan plan : plans) {
            ExecutionResult res = exec.execute(plan);
            // 打印或处理结果
        }
    }

}
