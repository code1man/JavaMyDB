package org.csu.mydb.storage.storageFiles.page.record;

import org.csu.mydb.storage.PageManager;

import java.nio.ByteBuffer;

//索引记录
public class IndexRecord {
    //记录头
    private RecordHead recordHead;

    //存放一个键值+子页指针
    private byte[] data;

    public IndexRecord(RecordHead recordHead, byte[] data) {
        this.recordHead = recordHead;
        this.data = data;
    }

    public RecordHead getRecordHead() {
        return recordHead;
    }

    public void setRecordHead(RecordHead recordHead) {
        this.recordHead = recordHead;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    /**
     * 序列化：将 IndexRecord 转为 byte[]
     */
    public byte[] toBytes() {
        // 计算总长度：RecordHead(4字节) + data.length
        int totalLength = 4 + (data != null ? data.length : 0);
        ByteBuffer buffer = ByteBuffer.allocate(totalLength);

        // 1. 写入 RecordHead
        buffer.put(recordHead.toBytes());

        // 2. 写入数据（可能为null）
        if (data != null) {
            buffer.put(data);
        }

        return buffer.array();
    }

    /**
     * 反序列化：从 byte[] 解析出 IndexRecord
     */
    public static IndexRecord fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length < 4) { // 最小长度：RecordHead(4)
            throw new IllegalArgumentException("Invalid IndexRecord data size");
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // 1. 解析 RecordHead
        byte[] headBytes = new byte[4];
        buffer.get(headBytes);
        RecordHead recordHead = RecordHead.fromBytes(headBytes);

        // 2. 解析剩余数据（如果有）
        byte[] data = null;
        if (buffer.hasRemaining()) {
            data = new byte[buffer.remaining()];
            buffer.get(data);
        }

        return new IndexRecord(recordHead, data);
    }
}
