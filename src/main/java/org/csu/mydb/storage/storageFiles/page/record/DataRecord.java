package org.csu.mydb.storage.storageFiles.page.record;

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
}
