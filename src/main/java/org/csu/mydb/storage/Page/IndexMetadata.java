package org.csu.mydb.storage.Page;

public class IndexMetadata {
    private final String indexName;    // 索引名称（如"PRIMARY"）
    private final String dataType;     // 键的数据类型（如"INT"、"VARCHAR"）
    private final int rootPageNo;      // B+树根节点页号
    private final int order;           // B+树阶数（控制节点键值数量）
    private final int spaceId;         // 表空间ID（关联数据页）

    public IndexMetadata(String indexName, String dataType, int rootPageNo, int order, int spaceId) {
        this.indexName = indexName;
        this.dataType = dataType;
        this.rootPageNo = rootPageNo;
        this.order = order;
        this.spaceId = spaceId;
    }

    // Getters
    public String getIndexName() { return indexName; }
    public String getDataType() { return dataType; }
    public int getRootPageNo() { return rootPageNo; }
    public int getOrder() { return order; }
    public int getSpaceId() { return spaceId; }
}
