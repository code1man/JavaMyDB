package org.csu.mydb.storage.BPlusTree;

public class SplitResult<K> {
    K newKey;       // 上浮的分割 key
    int newPageNo;  // 分裂出来的新节点 pageNo

    SplitResult(K newKey, int newPageNo) {
        this.newKey = newKey;
        this.newPageNo = newPageNo;
    }

    public SplitResult() {

    }
}
