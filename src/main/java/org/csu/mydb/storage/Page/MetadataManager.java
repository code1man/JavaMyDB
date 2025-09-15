package org.csu.mydb.storage.Page;

import java.util.HashMap;
import java.util.Map;

public class MetadataManager {
    // 模拟系统表存储的元数据（实际数据库从磁盘或内存系统表读取）
    private final Map<String, IndexMetadata> systemCatalog = new HashMap<>();

    public MetadataManager() {
        // 初始化示例元数据（实际数据库启动时加载）
        systemCatalog.put("PRIMARY", new IndexMetadata(
                "PRIMARY",
                "INT",
                3,    // 根页号（假设page3是根页）
                5,    // 阶数（order=5）
                1     // 表空间ID
        ));
    }

    // 根据索引名称查询元数据
    public IndexMetadata getIndexMetadata(String indexName) {
        return systemCatalog.get(indexName);
    }

    // 模拟创建新索引时的元数据写入（实际数据库写入系统表）
    public void createIndexMetadata(IndexMetadata metadata) {
        systemCatalog.put(metadata.getIndexName(), metadata);
    }
}