package org.csu.mydb.storage.Table.Column;

import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.util.Pair.Pair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 把 Column 与 Page 对接
 */
public class RecordSerializer {
    public static byte[] serialize(List<Object> values, List<Column> columns) {
        ByteBuffer buffer = ByteBuffer.allocate(4096); // 临时分配大块
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object val = values.get(i);

            if (val == null) {
                buffer.put((byte)0); // NULL 标志
                continue;
            } else {
                buffer.put((byte)1); // 非空
            }

            switch (col.getType().toUpperCase()) {
                case "INT":
                    buffer.putInt((Integer) val);
                    break;
                case "VARCHAR":
                    byte[] strBytes = ((String) val).getBytes(StandardCharsets.UTF_8);
                    buffer.putShort((short) strBytes.length);
                    buffer.put(strBytes);
                    break;
                case "DECIMAL":
                    buffer.putDouble((Double) val);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown type: " + col.getType());
            }
        }
        buffer.flip();
        byte[] result = new byte[buffer.limit()];
        buffer.get(result);
        return result;
    }

    public static List<Object> deserialize(byte[] record, List<Column> columns) {
        ByteBuffer buffer = ByteBuffer.wrap(record);
        List<Object> values = new ArrayList<>();
        for (Column col : columns) {
            byte flag = buffer.get(); // NULL 标志
            if (flag == 0) {
                values.add(null);
                continue;
            }

            switch (col.getType().toUpperCase()) {
                case "INT":
                    values.add(buffer.getInt());
                    break;
                case "VARCHAR":
                    short len = buffer.getShort();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    values.add(new String(strBytes, StandardCharsets.UTF_8));
                    break;
                case "DECIMAL":
                    values.add(buffer.getDouble());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown type: " + col.getType());
            }
        }
        return values;
    }

    /**
     * 内部节点序列化：保存Key和子页号
     * @param keyValues key值列表
     * @param keyColumns key对应列
     * @param childPageNo 子节点页号
     * @return byte[]
     */
    public static byte[] serializeKeyPtr(List<Object> keyValues, List<Column> keyColumns, int childPageNo) {
        byte[] keyData = serialize(keyValues, keyColumns); // 先序列化Key
        ByteBuffer buffer = ByteBuffer.allocate(keyData.length + 4); // 4字节存页号
        buffer.put(keyData);
        buffer.putInt(childPageNo); // 添加指针
        return buffer.array();
    }

    /**
     * 反序列化Key + ptr
     */
    public static Pair<Key, Integer> deserializeKeyPtr(byte[] data, List<Column> keyColumns) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        // 读取Key
        List<Object> keyValues = new ArrayList<>();
        for (Column col : keyColumns) {
            byte flag = buffer.get();
            if (flag == 0) {
                keyValues.add(null);
                continue;
            }
            switch (col.getType().toUpperCase()) {
                case "INT":
                    keyValues.add(buffer.getInt());
                    break;
                case "VARCHAR":
                    short len = buffer.getShort();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    keyValues.add(new String(strBytes, StandardCharsets.UTF_8));
                    break;
                case "DECIMAL":
                    keyValues.add(buffer.getDouble());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown type: " + col.getType());
            }
        }

        int childPageNo = buffer.getInt(); // 最后4字节是子节点页号
        Key key = new Key(keyValues, keyColumns);
        return new Pair<>(key, childPageNo);
    }

    /**
     * 反序列化叶子节点记录
     * @param recordData 页中存储的字节
     * @param keyColumns 用于生成 Key 的列
     * @return 对应行的列值列表
     */
    public static List<Object> deserializeRow(byte[] recordData, List<Column> keyColumns) {
        ByteBuffer buffer = ByteBuffer.wrap(recordData);
        List<Object> values = new ArrayList<>();

        for (Column col : keyColumns) {
            byte isNotNull = buffer.get();
            if (isNotNull == 0) {
                values.add(null);
                continue;
            }

            switch (col.getType().toUpperCase()) {
                case "INT":
                    values.add(buffer.getInt());
                    break;
                case "VARCHAR":
                    short len = buffer.getShort();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    values.add(new String(strBytes, StandardCharsets.UTF_8));
                    break;
                case "DECIMAL":
                    values.add(buffer.getDouble());
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown column type: " + col.getType());
            }
        }

        return values;
    }
}

