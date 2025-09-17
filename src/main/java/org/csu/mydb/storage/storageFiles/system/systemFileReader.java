package org.csu.mydb.storage.storageFiles.system;

import org.csu.mydb.storage.BPlusTree.BPlusTree;
import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Table;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class systemFileReader {
    private final PageManager pageManager;

    public systemFileReader(PageManager pageManager) {
        this.pageManager = pageManager;
    }

    /**
     * 获取数据库所有表及其列信息
     * @param databaseName 数据库名称
     * @param sysTablesFirstLeafPage sysTables系统表最左叶子结点
     * @param sysColumnsFirstLeafPage sysColumns系统表最左叶子结点
     * @return 数据库表信息映射表
     */
    public List<Table> getDatabaseTables(String databaseName, int sysTablesFirstLeafPage, int sysColumnsFirstLeafPage) throws IOException {
        // 1. 查询数据库的所有表
        List<sysTablesStructure> tables = queryTablesByDatabase(databaseName, sysTablesFirstLeafPage);

        // 2. 查询每个表的列信息
        List<Table> tableList = new ArrayList<>();
        for (sysTablesStructure table : tables) {

            List<Column> columns = queryColumnsByTable(table.getTableId(), sysColumnsFirstLeafPage);

            //加入缓存
            StorageSystem.spaceIdToColumns.put(table.getSpaceId(), columns);

            //构造需要的东西
            BPlusTree bPlusTree = new BPlusTree(table.getOrder(),
                    table.getSpaceId(),
                    new StorageSystem(),
                    columns,
                    "save/repos/" + databaseName + "/" + table.getTableName());

            List<String> columnNames = new ArrayList<>();
            for(Column column : columns){
                columnNames.add(column.getName());
            }

            Table returnTable = new Table(
                    table.getTableName(),
                    StorageSystem.path + table.getDatabaseName(),
                    columns,
                    table.getSpaceId(),
                    bPlusTree
            );

            tableList.add(returnTable);
        }

        return tableList;
    }

    /**
     * 查询指定数据库的所有表
     */
    private List<sysTablesStructure> queryTablesByDatabase(String databaseName, int sysTablesFirstLeafPage) throws IOException {
        List<sysTablesStructure> tables = new ArrayList<>();
        int currentPage = sysTablesFirstLeafPage;

//        PageManager.Page headPage = pageManager.getPage(StorageSystem.SYS_TABLES_IDB_SPACE_ID, 0);
        boolean isFirst = true;
//        while (currentPage != -1) {
//        for(int i = 0; i < ByteBuffer.wrap(headPage.getRecord(1)).getInt(); i++){
        while(currentPage != sysTablesFirstLeafPage || isFirst){
            PageManager.Page page = pageManager.getPage(StorageSystem.SYS_TABLES_IDB_SPACE_ID, currentPage);

            for (int slot = 0; slot < page.header.slotCount; slot++) {

                //判断槽位是否有效
                if(page.getSlots().get(slot).getStatus() != 1){
                    continue;
                }

                byte[] recordData = page.getRecord(slot);

                ByteBuffer buffer = ByteBuffer.wrap(recordData);
                // 跳到行数据
                buffer.position(12);

                ByteBuffer slicedBuffer = buffer.slice(); // 从当前 position 截取剩余数据
                byte[] remainingData = new byte[slicedBuffer.remaining()];
                slicedBuffer.get(remainingData);

                sysTablesStructure record = sysTablesStructure.fromBytes(remainingData);

                if (databaseName.equals(record.getDatabaseName())) {
                    tables.add(record);
                }
            }
            isFirst = false;
            // 获取下一页
            currentPage = page.header.nextPage;
        }
//        }

        return tables;
    }

    /**
     * 查询指定表的所有列
     */
    private List<Column> queryColumnsByTable(int tableId, int sysColumnsFirstLeafPage) throws IOException {
        List<Column> columns = new ArrayList<>();
        int currentPage = sysColumnsFirstLeafPage;

        PageManager.Page headPage = pageManager.getPage(StorageSystem.SYS_COLUMNS_IDB_SPACE_ID, 0);
        boolean isFirst = true;
//        while (currentPage != -1) {
        while(currentPage != sysColumnsFirstLeafPage || isFirst){
            PageManager.Page page = pageManager.getPage(StorageSystem.SYS_COLUMNS_IDB_SPACE_ID, currentPage);

            for (int slot = 0; slot < page.header.slotCount; slot++) {

                //判断槽位是否有效
                if(page.getSlots().get(slot).getStatus() != 1){
                    continue;
                }

                byte[] recordData = page.getRecord(slot);

                ByteBuffer buffer = ByteBuffer.wrap(recordData);
                // 跳到行数据
                buffer.position(12);

                ByteBuffer slicedBuffer = buffer.slice(); // 从当前 position 截取剩余数据
                byte[] remainingData = new byte[slicedBuffer.remaining()];
                slicedBuffer.get(remainingData);

                sysColumnsStructure record = sysColumnsStructure.fromBytes(remainingData);

                if (record.getTableId() == tableId) {
                    columns.add(convertToColumn(record));
                }
            }
            isFirst = false;
            // 获取下一页
            currentPage = page.header.nextPage;
        }

        // 按位置排序
        columns.sort(Comparator.comparingInt(Column::getPosition));
        return columns;
    }

    /**
     * 将sys_columns记录转换为Column对象
     */
    private Column convertToColumn(sysColumnsStructure record) {
        return new Column(
                record.getColumnName(),
                record.getType(),
                record.getLength(), // 注意：sysColumnsStructure中是short，Column中是int
                record.getScale(),  // 注意：sysColumnsStructure中是short，Column中是int
                record.getPosition(),
                record.isPrimaryKey(), // 主键
                record.isNullable(),
                record.getDefaultValue()
        );
    }
}
