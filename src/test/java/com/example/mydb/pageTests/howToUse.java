package com.example.mydb.pageTests;

import org.csu.mydb.storage.StorageSystem;
import org.csu.mydb.storage.Table.Table;
import org.csu.mydb.storage.storageFiles.system.sysColumnsStructure;
import org.csu.mydb.storage.storageFiles.system.sysTablesStructure;
import org.junit.jupiter.api.Test;

import java.util.List;

public class howToUse {

    //无需构造b+树
    @Test
    public void test1(){

        StorageSystem.loadSystemTable();

        sysTablesStructure e1 = new sysTablesStructure(4, "user", 4, 3, 2, "mydb");
        StorageSystem.insertIntoSysTable(e1);
        sysColumnsStructure column = new sysColumnsStructure();
        column.setColumnId(StorageSystem.allocateNewColumnId());
        column.setTableId(4);
        column.setColumnName("username");
        column.setType("varchar");
        column.setLength((short) 50);
        column.setScale((short) 0);       // 非小数类型设为0
        column.setPosition((short) 1);    // 第一列
        column.setNullable(false);
        column.setDefaultValue("default_user".getBytes());  // 字符串转字节数组
        column.setPrimaryKey(true);
        StorageSystem.insertIntoSysColumn(column);

        List<Table> tableList = StorageSystem.loadAllTables("mydb");

    }
}
