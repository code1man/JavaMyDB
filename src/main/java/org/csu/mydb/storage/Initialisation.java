package org.csu.mydb.storage;

import org.csu.mydb.storage.storageFiles.FileHeader;
import org.csu.mydb.storage.storageFiles.page.DataPage;
import org.csu.mydb.storage.storageFiles.page.IndexPage;
import org.csu.mydb.storage.storageFiles.page.record.DataRecord;
import org.csu.mydb.storage.storageFiles.page.record.RecordHead;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

//一台机器只用运行一次
//创建好ibdata1（系统表空间：存放系统表结构）
//创建好sys_tables.idb(存放所有表信息的文件)
//创建好sys_columns.idb(存放所有列信息的文件)
public class Initialisation {

    public static void main(String[] args) throws IOException {

        StorageSystem storageSystem = new StorageSystem();

        //创建好ibdata1（系统表空间：存放系统表结构）
        File file = new File("save/repos/ibdata1");
        // 创建文件头页
        PageManager.Page headerPage = new DataPage(0);
        //写入数据
        FileHeader fileHeader = new FileHeader(0, 6, -1, 3);
        List<byte[]> list = fileHeader.toBytesList();

        for (byte[] data : list){
            storageSystem.getPageManager().addRecord(0, 0, data);
        }

        //暂时不用
        PageManager.Page page1 = new DataPage(1);
        PageManager.Page page2 = new DataPage(2);
        //sys_tables结构
        PageManager.Page page3 = new IndexPage(3);
        //sys_columns结构
        PageManager.Page page4 = new IndexPage(4);
        //目前分配最大
        PageManager.Page page5 = new IndexPage(5);

        ByteBuffer newDataBuffer = ByteBuffer.allocate(4);
        newDataBuffer.putInt(5);
        RecordHead recordHead = new RecordHead((byte) 0, (byte) 0, (short) -1);
        DataRecord dataRecord = new DataRecord(recordHead, 0, 0, newDataBuffer.array());
        page5.addRecord(dataRecord.toBytes());



        //创建好sys_tables.idb(存放所有表信息的文件)
        File file1 = new File("save/repos/sys_tables.idb");
        // 创建文件头页
        PageManager.Page headerPage1 = new DataPage(0);
        //写入数据
        FileHeader fileHeader1 = new FileHeader(1, 4, 3, -1);
        List<byte[]> list1 = fileHeader1.toBytesList();

        for (byte[] data : list1){
            storageSystem.getPageManager().addRecord(1, 0, data);
        }

        //暂时不用
        PageManager.Page page21 = new DataPage(1);
        PageManager.Page page22 = new DataPage(2);
        //索引根节点
        PageManager.Page page23 = new IndexPage(3);


        //创建好sys_columns.idb(存放所有列信息的文件)
        File file2 = new File("save/repos/sys_columns.idb");
        // 创建文件头页
        PageManager.Page headerPage2 = new DataPage(0);
        //写入数据
        FileHeader fileHeader2 = new FileHeader(2, 4, 3, -1);
        List<byte[]> list2 = fileHeader2.toBytesList();

        for (byte[] data : list2){
            storageSystem.getPageManager().addRecord(2, 0, data);
        }

        //暂时不用
        PageManager.Page page31 = new DataPage(1);
        PageManager.Page page32 = new DataPage(2);
        //索引根节点
        PageManager.Page page33 = new IndexPage(3);
    }
}
