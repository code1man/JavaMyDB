package org.csu.mydb.storage.storageFiles.page;

import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.storage.storageFiles.page.record.RecordHead;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class KeyExtractor {

    /**
     * 从记录中提取主键值
     * @param recordData 记录原始数据
     * @param pageType 页类型
     * @param allColumns 所有列定义（用于数据页）
     * @param primaryKeyColumns 主键列定义（用于索引页）
     * @return 主键对象
     */
    public static Key extractPrimaryKey(byte[] recordData, byte pageType,
                                        List<Column> allColumns, List<Column> primaryKeyColumns) {
        if (pageType == PageType.DATA_PAGE) {
            return extractPrimaryKeyFromDataRecord(recordData, allColumns);
        } else if (pageType == PageType.INDEX_PAGE) {
            return extractPrimaryKeyFromIndexRecord(recordData, primaryKeyColumns);
        } else {
            throw new IllegalArgumentException("Unsupported page type: " + pageType);
        }
    }

    /**
     * 从数据页记录中提取主键值
     */
    private static Key extractPrimaryKeyFromDataRecord(byte[] recordData, List<Column> allColumns) {
        ByteBuffer buffer = ByteBuffer.wrap(recordData);

        // 跳过记录头
        buffer.position(RecordHead.RECORD_HEADER_SIZE);

        // 解析事务ID和回滚指针
        int transactionId = buffer.getInt();
        int rollPointer = buffer.getInt();

        // 提取行数据
        byte[] rowData = new byte[buffer.remaining()];
        buffer.get(rowData);

        // 从行数据中提取主键值
        return extractPrimaryKeyFromRowData(rowData, allColumns);
    }

    /**
     * 从行数据中提取主键值
     */
    private static Key extractPrimaryKeyFromRowData(byte[] rowData, List<Column> allColumns) {
        ByteBuffer buffer = ByteBuffer.wrap(rowData);
        List<Object> keyValues = new ArrayList<>();
        List<Column> pkColumns = new ArrayList<>();

        for (Column col : allColumns) {
            if (!col.isPrimaryKey()) {
                // 跳过非主键列
                skipColumn(buffer, col);
                continue;
            }

            // 提取主键列值
            Object value = readColumnValue(buffer, col);
            keyValues.add(value);
            pkColumns.add(col);
        }

        return new Key(keyValues, pkColumns);
    }

    /**
     * 从索引页记录中提取主键值
     */
    private static Key extractPrimaryKeyFromIndexRecord(byte[] recordData, List<Column> primaryKeyColumns) {
        ByteBuffer buffer = ByteBuffer.wrap(recordData);

        // 跳过记录头
        buffer.position(RecordHead.RECORD_HEADER_SIZE);

        // 提取主键值
        List<Object> keyValues = new ArrayList<>();
        for (Column col : primaryKeyColumns) {
            byte flag = buffer.get(); // NULL标志
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
                    throw new UnsupportedOperationException("Unsupported type: " + col.getType());
            }
        }

        return new Key(keyValues, primaryKeyColumns);
    }

    /**
     * 读取列值
     */
    private static Object readColumnValue(ByteBuffer buffer, Column col) {
        byte flag = buffer.get(); // NULL标志
        if (flag == 0) return null;

        switch (col.getType().toUpperCase()) {
            case "INT":
                return buffer.getInt();
            case "VARCHAR":
                short len = buffer.getShort();
                byte[] strBytes = new byte[len];
                buffer.get(strBytes);
                return new String(strBytes, StandardCharsets.UTF_8);
            case "DECIMAL":
                return buffer.getDouble();
            case "BOOLEAN":
                return buffer.get() == 1;
            case "DATE":
                return buffer.getLong(); // 时间戳表示
            default:
                throw new UnsupportedOperationException("Unsupported type: " + col.getType());
        }
    }

    /**
     * 跳过非主键列
     */
    private static void skipColumn(ByteBuffer buffer, Column col) {
        byte flag = buffer.get(); // NULL标志
        if (flag == 0) return; // NULL值不占空间

        switch (col.getType().toUpperCase()) {
            case "INT":
                buffer.position(buffer.position() + 4);
                break;
            case "VARCHAR":
                short len = buffer.getShort();
                buffer.position(buffer.position() + len);
                break;
            case "DECIMAL":
                buffer.position(buffer.position() + 8);
                break;
            case "BOOLEAN":
                buffer.position(buffer.position() + 1);
                break;
            case "DATE":
                buffer.position(buffer.position() + 8);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported type: " + col.getType());
        }
    }
}