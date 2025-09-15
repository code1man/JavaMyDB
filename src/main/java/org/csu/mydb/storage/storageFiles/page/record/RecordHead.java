package org.csu.mydb.storage.storageFiles.page.record;

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

}
