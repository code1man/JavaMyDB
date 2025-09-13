package org.csu.mydb.storage;

import org.csu.mydb.storage.bufferPool.BufferPool;

import java.util.HashMap;
import java.util.Map;

public class DBMS {
    public static BufferPool BUFFER_POOL = new BufferPool(1024); // 全局共享

    private static final Map<String, Table> tables = new HashMap<>();

    public static void registerTable(Table table) {
        tables.put(table.getName(), table);
    }

    public static Table getTable(String name) {
        return tables.get(name);
    }
}

