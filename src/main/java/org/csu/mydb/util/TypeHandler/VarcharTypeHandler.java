package org.csu.mydb.util.TypeHandler;

import java.io.IOException;
import java.nio.ByteBuffer;

// String类型处理器（2字节长度前缀+UTF-8内容）
public class VarcharTypeHandler implements TypeHandler<String> {
    @Override
    public String deserialize(ByteBuffer buffer){
        int length = buffer.getShort() & 0xFFFF; // 读取2字节无符号长度
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public int getStorageSize(String value) {
        byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return 2 + bytes.length; // 长度前缀2字节 + 内容长度
    }

    @Override
    public byte[] serialize(String key) {
        return new byte[0];
    }
}
