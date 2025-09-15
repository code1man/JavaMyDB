package org.csu.mydb.storage;

import org.csu.mydb.storage.bufferPool.BufferPool;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
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
     * @param tableName 表名
     * @param columns   列的数据
     */
    public static void createTable(String tableName, List<Column> columns){

        //先分配spaceId
        //找到sys_tables.idb文件
        //先看换缓存有没有
        RandomAccessFile raf = pageManager.getOpenFiles().get(SYS_TABLES_IDB_SPACE_ID);
        //缓存没有的话
        if(raf == null){
            File file = new File(path + "sys_tables.idb");
            try {
                raf = new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        //从第四页开始读

        //每个页找到spaceId字段

        //拿到他的数据区

        //新建文件
        pageManager.openFile();

        //初始化文件

        //写入
    }

}