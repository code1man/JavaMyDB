package org.csu.mydb.executor;


import org.csu.mydb.cli.ParsedCommand;
import org.csu.mydb.storage.StorageEngine;
import org.csu.mydb.executor.storage.Tuple;
import java.util.ArrayList;
import java.util.List;

/**
 * 执行引擎入口，负责执行算子树
 * 遵循迭代器模型：open() → hasNext() → getNext() → close()
 */
public class ExecutionEngine {

    /**
     * 执行查询计划
     * @param root 查询计划的根节点算子
     * @return 查询结果的所有元组
     */
    public List<Tuple> execute(Operation root) {
        List<Tuple> result = new ArrayList<>();

        try {
            // 1. 打开算子树（初始化资源）
            root.open();

            // 2. 迭代获取所有元组
            while (root.hasNext()) {
                Tuple tuple = root.getNext();
                if (tuple != null) {
                    result.add(tuple);
                }
            }

            // 3. 返回结果
            return result;
        } finally {
            // 4. 确保关闭算子树（释放资源）
            root.close();
        }
    }

    /**
     * 执行查询计划并打印结果（用于调试）
     * @param root 查询计划的根节点算子
     */
    public void executeAndPrint(Operation root) {
        List<Tuple> result = execute(root);

        // 打印属性名（表头）
        List<String> attributes = root.getAttributeList();
        System.out.println(String.join("\t", attributes));

        // 打印分隔线
        System.out.println("----------------------------------------");

        // 打印所有元组
        for (Tuple tuple : result) {
            System.out.println(tuple.toString());
        }
    }
}