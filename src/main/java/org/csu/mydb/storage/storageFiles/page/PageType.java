package org.csu.mydb.storage.storageFiles.page;

public class PageType {
    public static final byte INDEX_PAGE = 1;  // 索引页（存储B+树节点）
    public static final byte DATA_PAGE = 0;   // 数据页（存储实际行数据）
}
