package org.csu.mydb.storage.storageFiles.page.record;

import java.nio.ByteBuffer;

//记录头
public class RecordHead {

    public static int RECORD_HEADER_SIZE = 4;

    //1字节，是否删除标记（1代表已删除）
    private byte isDeleted;

    //1字节，记录类型（0代表普通数据，1的代表索引记录）
    private byte recordType;

    //2字节，下一个记录的偏移量
    private short nextRecord;

    public byte isDeleted() {
        return isDeleted;
    }

    public void setDeleted(byte deleted) {
        isDeleted = deleted;
    }

    public byte isRecordType() {
        return recordType;
    }

    public void setRecordType(byte recordType) {
        this.recordType = recordType;
    }

    public short getNextRecord() {
        return nextRecord;
    }

    public void setNextRecord(short nextRecord) {
        this.nextRecord = nextRecord;
    }

    public RecordHead(byte isDeleted, byte recordType, short nextRecord) {
        this.isDeleted = isDeleted;
        this.recordType = recordType;
        this.nextRecord = nextRecord;
    }

    /**
     * 序列化：将 RecordHead 转为 byte[]（固定4字节）
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4); // 1 + 1 + 2 = 4字节
        buffer.put(isDeleted);    // 直接写入 byte（无需转换）
        buffer.put(recordType);   // 直接写入 byte
        buffer.putShort(nextRecord);
        return buffer.array();
    }

    /**
     * 反序列化：从 byte[] 解析出 RecordHead
     */
    public static RecordHead fromBytes(byte[] data) {
        if (data == null || data.length != 4) {
            throw new IllegalArgumentException("Invalid RecordHead data size. Expected 4 bytes, got " + (data != null ? data.length : "null"));
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte isDeleted = buffer.get();      // 读取1字节
        byte recordType = buffer.get();     // 读取1字节
        short nextRecord = buffer.getShort(); // 读取2字节
        return new RecordHead(isDeleted, recordType, nextRecord);
    }

}
