package org.csu.mydb.executor;



import java.util.List;
import org.csu.mydb.executor.storage.Schema;
import org.csu.mydb.executor.storage.Tuple;
import org.csu.mydb.cli.ParsedCommand;
import org.csu.mydb.storage.StorageEngine;
/**
 * 所有执行算子的通用接口
 * 遵循迭代器模型：open() → hasNext() → getNext() → close()
 */
public interface Operation {

    /**
     * 初始化算子，分配资源
     */
    void open();

    /**
     * 检查是否还有下一个元组
     * @return 如果还有元组返回true，否则false
     */
    boolean hasNext();

    /**
     * 获取下一个元组
     * @return 下一个元组，如果没有返回null
     */
    Tuple getNext();

    /**
     * 关闭算子，释放资源
     */
    void close();

    /**
     * 获取算子的输出模式
     * @return 输出模式Schema对象
     */
    Schema getSchema();

    /**
     * 获取算子的输出属性列表
     * @return 属性名称列表
     */
    List<String> getAttributeList();
}