package org.csu.mydb.storage.storageFiles.page.record;

import java.nio.ByteBuffer;

//记录头
public class RecordHead {

    //1字节，是否删除标记（1代表已删除）
    private boolean isDeleted;

    //1字节，记录类型（0代表普通数据，1的代表索引记录）
    private boolean recordType;

    //2字节，下一个记录的偏移量
    private short nextRecord;

    public boolean isDeleted() {
        return isDeleted;
    }

    public void setDeleted(boolean deleted) {
        isDeleted = deleted;
    }

    public boolean isRecordType() {
        return recordType;
    }

    public void setRecordType(boolean recordType) {
        this.recordType = recordType;
    }

    public short getNextRecord() {
        return nextRecord;
    }

    public void setNextRecord(short nextRecord) {
        this.nextRecord = nextRecord;
    }

    public RecordHead(boolean isDeleted, boolean recordType, short nextRecord) {
        this.isDeleted = isDeleted;
        this.recordType = recordType;
        this.nextRecord = nextRecord;
    }


    /**
     * 序列化：将 RecordHead 转为 byte[]
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(4); // 1(bool) + 1(bool) + 2(short) = 4字节
        // 注意：Java 的 boolean 在 ByteBuffer 中需手动转为 1/0
        buffer.put((byte) (isDeleted ? 1 : 0));
        buffer.put((byte) (recordType ? 1 : 0));
        buffer.putShort(nextRecord);
        return buffer.array();
    }

    /**
     * 反序列化：从 byte[] 解析出 RecordHead
     */
    public static RecordHead fromBytes(byte[] data) {
        if (data == null || data.length < 4) {
            throw new IllegalArgumentException("Invalid RecordHead data size");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        boolean isDeleted = buffer.get() == 1;
        boolean recordType = buffer.get() == 1;
        short nextRecord = buffer.getShort();
        return new RecordHead(isDeleted, recordType, nextRecord);
    }

}
