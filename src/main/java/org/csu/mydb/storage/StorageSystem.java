package org.csu.mydb.storage;

import org.csu.mydb.storage.bufferPool.BufferPool;
import org.csu.mydb.storage.storageFiles.page.PageOperations;
import org.csu.mydb.storage.storageFiles.page.record.DataRecord;
import org.csu.mydb.storage.storageFiles.page.record.RecordHead;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.csu.mydb.storage.PageManager.PAGE_SIZE;

//组合缓存和页管理（全局只能有一个的东西，比如缓存）
public class StorageSystem implements PageOperations {

    //系统文件表空间的spaceId
    private static final int IBDATA1_SPACE_ID = 0;
    private static final int SYS_TABLES_IDB_SPACE_ID = 1;
    private static final int SYS_COLUMNS_IDB_SPACE_ID = 2;
    private static final String path = "save/repos/";
    private static final PageManager pageManager = new PageManager();
    private static final BufferPool bufferPool = new BufferPool(150, pageManager);

    public StorageSystem() {
//        this.pageManager = new PageManager();
//        this.bufferPool = new BufferPool(150, pageManager);
        pageManager.setBufferPool(bufferPool);
    }


    public PageManager getPageManager() {
        return pageManager;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    //========================== 存储系统的静态方法（比存储引擎低一层的方法） ============================//

    @Override
    public byte[] readPage(String filePath, int spaceId, int pageNo) {
        try {
            // 确保文件已打开
            if (!pageManager.getOpenFiles().containsKey(spaceId)) {
                pageManager.openFile(spaceId, filePath);
            }

            // 从缓存或磁盘读取页
            PageManager.Page page = pageManager.getPage(spaceId, pageNo);
            return page.toBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read page", e);
        }
    }

    @Override
    public void writePage(String filePath, int spaceId, int pageNo, byte[] data, List<Column> columns) {
        try {
            // 确保文件已打开
            if (!pageManager.getOpenFiles().containsKey(spaceId)) {
                pageManager.openFile(spaceId, filePath);
            }

            //获得页
            PageManager.Page page = pageManager.getPage(spaceId, pageNo);

            //分类讨论
            int pageType = page.header.pageType;
            //如果是数据页
            if(pageType == 0){
                //构造记录
                RecordHead recordHead = new RecordHead((byte) 0, (byte) 0, (short)-1);
                DataRecord dataRecord = new DataRecord(recordHead, 0, 0, data);

            }else {

            }

            // 写入缓存
            bufferPool.putPage(page, spaceId);

        } catch (IOException e) {
            throw new RuntimeException("Failed to write page", e);
        }
    }

//    @Override
//    public int getPageType(byte[] pageBytes) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        return page.header.pageType;
//    }
//
//    @Override
//    public byte[] getDataPageData(byte[] pageBytes) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//
//        // 仅返回数据区内容（不包括页头和槽位）
//        int dataStart = page.dataStartOffset;
//        int dataEnd = PageManager.PAGE_SIZE;
//        return Arrays.copyOfRange(page.pageData, dataStart, dataEnd);
//    }
//
//    @Override
//    public byte[] getIndexPageData(byte[] pageBytes) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//
//        // 索引页数据包括键值和指针
//        int indexDataStart = PageManager.PAGE_HEADER_SIZE;
//        int indexDataEnd = page.dataStartOffset;
//        return Arrays.copyOfRange(page.pageData, indexDataStart, indexDataEnd);
//    }
//
//
//    @Override
//    public PageHeader getPageHeader(byte[] pageBytes) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        return page.header;
//    }
//
//    @Override
//    public byte[] updatePageHeader(byte[] pageBytes, PageHeader newHeader) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        page.header = newHeader;
//        return page.toBytes();
//    }
//
//    @Override
//    public int getFreeSpace(byte[] pageBytes) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        return page.header.freeSpace;
//    }
//
//
//    @Override
//    public byte[] addRecordToPage(byte[] pageBytes, byte[] recordData) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        boolean success = page.addRecord(recordData);
//        if (!success) {
//            throw new PageFullException("Page has no space for new record");
//        }
//        return page.toBytes();
//    }
//
//    @Override
//    public byte[] updateRecordInPage(byte[] pageBytes, int slotIndex, byte[] newRecord) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        boolean success = page.updateRecord(slotIndex, newRecord);
//        if (!success) {
//            throw new RecordUpdateException("Failed to update record");
//        }
//        return page.toBytes();
//    }
//
//    @Override
//    public byte[] deleteRecordFromPage(byte[] pageBytes, int slotIndex) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        boolean success = page.freeRecord(slotIndex);
//        if (!success) {
//            throw new RecordNotFoundException("Record not found");
//        }
//        return page.toBytes();
//    }
//
//    @Override
//    public byte[] getRecordFromPage(byte[] pageBytes, int slotIndex) {
//        PageManager.Page page = PageManager.Page.fromBytes(pageBytes);
//        return page.getRecord(slotIndex);
//    }
    /**
     * 创建数据库
     *暂时不需要考虑
     */
    public static void createDB(){

    }

    /**
     * 创建表格，初始化文件
     * @param filePath 包括文件本身idb
     * @param columns   列的数据
     * 返回分配的 spaceId
     */
    public static int createTable(String filePath, List<Column> columns){

        //先分配spaceId
        //找到ibdata1文件//读第五页
        PageManager.Page page ;
        try {
            page = pageManager.readPage(IBDATA1_SPACE_ID, 5);

            //只存了一个数据
            DataRecord dataRecord = DataRecord.fromBytes(ByteBuffer.wrap(page.getRecord(0)).array());
            int maxSpaceId = ByteBuffer.wrap(dataRecord.getData()).getInt();

            //新建文件 //初始化文件
            pageManager.openFile(maxSpaceId + 1, filePath);

            // 4. 将 newSpaceId 写回原始位置
            // 4.1 构造新的数据（假设原数据只有 maxSpaceId，现在更新它）
            ByteBuffer newDataBuffer = ByteBuffer.allocate(4); // int 占 4 字节
            newDataBuffer.putInt(maxSpaceId + 1);
            byte[] newData = newDataBuffer.array();

            // 4.2 构造新的 DataRecord（假设 DataRecord 结构不变）
            DataRecord newDataRecord = new DataRecord(
                    dataRecord.getRecordHead(),
                    dataRecord.getTransactionId(),
                    dataRecord.getRollPointer(),
                    newData                  // 更新数据部分
            );

            // 4.3 将 DataRecord 序列化为字节
            byte[] updatedRecordBytes = newDataRecord.toBytes();

            // 4.4 更新页中的记录（假设 page.updateRecord 存在）
            page.updateRecord(0, updatedRecordBytes);

            bufferPool.putPage(page, 0);

            return maxSpaceId + 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //这个按道理来说也应该使用b+树去找
//    public List<Column> getColumnsBySpaceId(int spaceId) throws IOException {
//        // 1. 获取系统表空间ID（假设固定为0）
//        int sysSpaceId = 0;
//
//        // 2. 获取sys_tables表根页（假设固定为3）
//        int sysTablesRoot = 3;
//
//        // 3. 在sys_tables中查找spaceId对应的表ID
//        int tableId = findTableIdBySpaceId(sysSpaceId, sysTablesRoot, spaceId);
//        if (tableId == -1) {
//            throw new TableNotFoundException("Table not found for spaceId: " + spaceId);
//        }
//
//        // 4. 获取sys_columns表根页（假设固定为4）
//        int sysColumnsRoot = 4;
//
//        // 5. 查询所有列信息
//        return findColumnsByTableId(sysSpaceId, sysColumnsRoot, tableId);
//    }

}