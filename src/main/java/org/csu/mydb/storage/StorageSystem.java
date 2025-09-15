package org.csu.mydb.storage;

import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.bufferPool.BufferPool;
import org.csu.mydb.storage.storageFiles.page.record.DataRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

//组合缓存和页管理（全局只能有一个的东西，比如缓存）
public class StorageSystem {

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
            return maxSpaceId + 1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}