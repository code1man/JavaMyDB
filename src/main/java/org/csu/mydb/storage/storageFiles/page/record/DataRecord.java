package org.csu.mydb.storage.storageFiles.page.record;

import java.nio.ByteBuffer;

//数据页的记录
public class DataRecord {
    //记录头
    private RecordHead recordHead;

    //事务id（目前不会用到）
    private int transactionId;

    //回滚指针（目前也不会用到）
    private int rollPointer;

    //行数据
    private byte[] data;

    public DataRecord(RecordHead recordHead, int transactionId, int rollPointer, byte[] data) {
        this.recordHead = recordHead;
        this.transactionId = transactionId;
        this.rollPointer = rollPointer;
        this.data = data;
    }

    public RecordHead getRecordHead() {
        return recordHead;
    }

    public void setRecordHead(RecordHead recordHead) {
        this.recordHead = recordHead;
    }

    public int getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(int transactionId) {
        this.transactionId = transactionId;
    }

    public int getRollPointer() {
        return rollPointer;
    }

    public void setRollPointer(int rollPointer) {
        this.rollPointer = rollPointer;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * 序列化：将 DataRecord 转为 byte[]
     */
    public byte[] toBytes() {
        // 计算总长度：RecordHead(4) + transactionId(4) + rollPointer(4) + data.length
        int totalLength = 4 + 4 + 4 + (data != null ? data.length : 0);
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // 1. 写入 RecordHead
        buffer.put(recordHead.toBytes());

        // 2. 写入 transactionId 和 rollPointer
        buffer.putInt(transactionId);
        buffer.putInt(rollPointer);

        // 3. 写入数据（可能为null）
        if (data != null) {
            buffer.put(data);
        }

        return buffer.array();
    }

    /**
     * 反序列化：从 byte[] 解析出 DataRecord
     */
    public static DataRecord fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length < 12) { // 最小长度：4(RecordHead) + 4 + 4 = 12
            throw new IllegalArgumentException("Invalid DataRecord data size");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // 1. 解析 RecordHead
        byte[] headBytes = new byte[4];
        buffer.get(headBytes);
        RecordHead recordHead = RecordHead.fromBytes(headBytes);

        // 2. 解析 transactionId 和 rollPointer
        int transactionId = buffer.getInt();
        int rollPointer = buffer.getInt();

        // 3. 解析剩余数据（如果有）
        byte[] data = null;
        if (buffer.hasRemaining()) {
            data = new byte[buffer.remaining()];
            buffer.get(data);
        }

        return new DataRecord(recordHead, transactionId, rollPointer, data);
    }


}
