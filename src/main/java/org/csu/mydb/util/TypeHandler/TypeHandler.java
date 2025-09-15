package org.csu.mydb.util.TypeHandler;

import java.nio.ByteBuffer;
import java.io.IOException;

public interface TypeHandler<K extends Comparable<K>> {
    // 从ByteBuffer反序列化为K类型
    K deserialize(ByteBuffer buffer);

    // 计算K类型值在页中的存储长度（用于写入时预留空间）
    int getStorageSize(K value);

    byte[] serialize(K key);

}