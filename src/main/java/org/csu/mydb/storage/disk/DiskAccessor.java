package org.csu.mydb.storage.disk;

import org.csu.mydb.storage.PageManager;

import java.io.IOException;

//磁盘访问接口
public interface DiskAccessor {
    PageManager.Page readPage(int spaceId, int pageNo) throws IOException;
    void writePage(int spaceId, int pageNo, PageManager.Page page) throws IOException;
}
