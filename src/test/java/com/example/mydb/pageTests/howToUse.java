package com.example.mydb.pageTests;

import org.csu.mydb.storage.StorageSystem;
import org.junit.jupiter.api.Test;

public class howToUse {

    //无需构造b+树
    @Test
    public void test1(){
        String dbName = "数据库名字";
        //获取所有表格（这里3指的是系统表存储数据的页号，只存在第三页）
//        StorageSystem.loadAllTables(dbName, 3,3);

        String filePath = "save/repos/sys_tables.idb";
        //往系统表插入数据
//        StorageSystem.writePage(filePath, 1, 3, )

    }
}
