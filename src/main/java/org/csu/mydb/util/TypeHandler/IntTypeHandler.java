package org.csu.mydb.util.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

// Integer类型处理器（4字节大端）
public class IntTypeHandler implements TypeHandler<Integer> {
    @Override
    public Integer deserialize(ByteBuffer buffer){
        return buffer.getInt(); // 直接读取4字节大端整数
    }

    @Override
    public int getStorageSize(Integer value) {
        return 4; // INT固定占4字节
    }

    @Override
    public byte[] serialize(Integer key) {
        return new byte[0];
    }
}