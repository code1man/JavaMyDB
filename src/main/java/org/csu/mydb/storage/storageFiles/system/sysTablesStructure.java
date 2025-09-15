package org.csu.mydb.storage.storageFiles.system;

//系统表sys_tables结构的硬编码
public class sysTablesStructure {
    private int tableId;
    private String tableName;
    private int spaceId;
    private int rootPage;

    //b+树层数
    private int order;

//    //逻辑删除标记
//    private boolean isDeleted;

    public sysTablesStructure(int tableId, String tableName, int spaceId, int rootPage, int order) {
        this.tableId = tableId;
        this.tableName = tableName;
        this.spaceId = spaceId;
        this.rootPage = rootPage;
        this.order = order;
//        this.isDeleted = isDeleted;
    }

    public int getTableId() {
        return tableId;
    }

    public void setTableId(int tableId) {
        this.tableId = tableId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    public int getRootPage() {
        return rootPage;
    }

    public void setRootPage(int rootPage) {
        this.rootPage = rootPage;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

//    public boolean isDeleted() {
//        return isDeleted;
//    }
//
//    public void setDeleted(boolean deleted) {
//        isDeleted = deleted;
//    }
}
