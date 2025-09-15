package org.csu.mydb.storage.storageFiles.page.record;

import org.csu.mydb.storage.PageManager;

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
}
