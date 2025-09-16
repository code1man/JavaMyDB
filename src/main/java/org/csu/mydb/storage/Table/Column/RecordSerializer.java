package org.csu.mydb.storage.Table.Column;

import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.storage.storageFiles.page.record.IndexRecord;
import org.csu.mydb.storage.storageFiles.page.record.RecordHead;
import org.csu.mydb.util.Pair.Pair;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

/**
 * 把 Column 与 Page 对接
 */
public class RecordSerializer {
    // ---------------- 数据页 ----------------

    /**
     * 序列化数据页中的一行记录（含 RecordHead + txnId + rollbackPtr + 列数据）
     */
    public static byte[] serializeDataRow(List<Object> values, List<Column> columns
                                          // int transactionId, int rollbackPointer
    ) {
        // 先序列化列数据
        ByteBuffer rowBuffer = ByteBuffer.allocate(1024); // 临时缓冲，可根据列数动态扩展
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            Object val = values.get(i);

            if (val == null) rowBuffer.put((byte)0);
            else {
                rowBuffer.put((byte)1);
                switch (col.getType().toUpperCase()) {
                    case "INT": rowBuffer.putInt((Integer) val); break;
                    case "VARCHAR":
                        byte[] strBytes = ((String) val).getBytes(StandardCharsets.UTF_8);
                        rowBuffer.putShort((short) strBytes.length);
                        rowBuffer.put(strBytes);
                        break;
                    case "DECIMAL": rowBuffer.putDouble((Double) val); break;
                    case "BOOLEAN": rowBuffer.put((byte)((Boolean) val ? 1 : 0)); break;
                    case "DATE": rowBuffer.putLong(((Date) val).getTime()); break;
                    default: throw new UnsupportedOperationException("Unknown type: " + col.getType());
                }
            }
        }
        rowBuffer.flip();
        byte[] rowData = new byte[rowBuffer.limit()];
        rowBuffer.get(rowData);

        // 添加记录头 + 事务ID + rollbackPointer
        ByteBuffer buffer = ByteBuffer.allocate(RecordHead.RECORD_HEADER_SIZE + 8 + rowData.length);
        RecordHead head = new RecordHead((byte)0, (byte)0, (short)-1);
        buffer.put(head.toBytes());
        buffer.putInt(0);
        buffer.putInt(0);
        buffer.put(rowData);

        return buffer.array();
    }

    /**
     * 反序列化数据页记录，返回完整列值
     */
    public static List<Object> deserializeDataRow(byte[] recordData, List<Column> columns) {
        ByteBuffer buffer = ByteBuffer.wrap(recordData);

        // 跳过记录头
        buffer.position(RecordHead.RECORD_HEADER_SIZE);

        // 跳过事务ID + rollback pointer
        buffer.getInt(); // transactionId
        buffer.getInt(); // rollbackPointer

        List<Object> values = new ArrayList<>();
        for (Column col : columns) {
            byte isNotNull = buffer.get();
            if (isNotNull == 0) {
                values.add(null);
                continue;
            }

            switch (col.getType().toUpperCase()) {
                case "INT": values.add(buffer.getInt()); break;
                case "VARCHAR":
                    short len = buffer.getShort();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    values.add(new String(strBytes, StandardCharsets.UTF_8));
                    break;
                case "DECIMAL": values.add(buffer.getDouble()); break;
                case "BOOLEAN": values.add(buffer.get() == 1); break;
                case "DATE": values.add(new Date(buffer.getLong())); break;
                default: throw new UnsupportedOperationException("Unknown type: " + col.getType());
            }
        }

        return values;
    }

    // ---------------- 索引页 ----------------
    /**
     * 反序列化Key + ptr
     *
     * 兼容三种常见布局：
     *  - 纯 payload（从偏移 0 开始）
     *  - 带 RecordHead（从 RecordHead.RECORD_HEADER_SIZE 开始）
     *  - 带 RecordHead + txnId + rollbackPtr（从 RecordHead.RECORD_HEADER_SIZE + 8 开始）
     *
     * 如果解析失败会抛出 IOException。
     */
    public static byte[] serializeKeyPtr(List<Object> keyValues, List<Column> keyColumns, int childPageNo) {
        ByteBuffer buffer = ByteBuffer.allocate(256); // 可以动态调整
        for (int i = 0; i < keyColumns.size(); i++) {
            Column col = keyColumns.get(i);
            Object val = keyValues.get(i);

            if (val == null) buffer.put((byte)0);
            else {
                buffer.put((byte)1);
                switch (col.getType().toUpperCase()) {
                    case "INT": buffer.putInt((Integer) val); break;
                    case "VARCHAR":
                        byte[] b = ((String) val).getBytes(StandardCharsets.UTF_8);
                        buffer.putShort((short)b.length);
                        buffer.put(b);
                        break;
                    case "DECIMAL": buffer.putDouble((Double) val); break;
                    case "BOOLEAN": buffer.put((byte)((Boolean) val ? 1 : 0)); break;
                    case "DATE": buffer.putLong(((Date) val).getTime()); break;
                    default: throw new UnsupportedOperationException("Unknown type: " + col.getType());
                }
            }
        }

        // 最后写子页指针
        buffer.putInt(childPageNo);

        buffer.flip();
        byte[] dataWithoutHead = new byte[buffer.limit()];
        buffer.get(dataWithoutHead);

        // 包装 RecordHead
        RecordHead head = new RecordHead((byte)0, (byte)1, (short)-1); // recordType=1 表示索引记录
        IndexRecord indexRecord = new IndexRecord(head, dataWithoutHead);
        return indexRecord.toBytes();
    }

    public static Pair<Key, Integer> deserializeKeyPtr(byte[] bytes, List<Column> keyColumns) {
        IndexRecord record = IndexRecord.fromBytes(bytes);
        ByteBuffer buffer = ByteBuffer.wrap(record.getData()); // 使用 data 部分

        List<Object> keyValues = new ArrayList<>();
        for (Column col : keyColumns) {
            byte flag = buffer.get();
            if (flag == 0) { keyValues.add(null); continue; }

            switch (col.getType().toUpperCase()) {
                case "INT": keyValues.add(buffer.getInt()); break;
                case "VARCHAR":
                    short len = buffer.getShort();
                    byte[] strBytes = new byte[len];
                    buffer.get(strBytes);
                    keyValues.add(new String(strBytes, StandardCharsets.UTF_8));
                    break;
                case "DECIMAL": keyValues.add(buffer.getDouble()); break;
                case "BOOLEAN": keyValues.add(buffer.get() == 1); break;
                case "DATE": keyValues.add(new Date(buffer.getLong())); break;
                default: throw new UnsupportedOperationException("Unknown type: " + col.getType());
            }
        }

        int childPageNo = buffer.getInt();
        Key key = new Key(keyValues, keyColumns);
        return new Pair<>(key, childPageNo);
    }
}

