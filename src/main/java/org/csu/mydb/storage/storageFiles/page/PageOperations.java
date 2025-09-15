package org.csu.mydb.storage.storageFiles.page;

import org.csu.mydb.storage.Column;
import org.csu.mydb.storage.PageManager;

import java.util.List;

public interface PageOperations {
    // 读取页
    byte[] readPage(String filePath, int spaceId, int pageNo);

    // 写入页
    void writePage(String filePath, int spaceId, int pageNo, byte[] pageData, List<Column> columns);

    /*// 获取页类型
    int getPageType(byte[] pageBytes);

    // 获取数据页内容
    byte[] getDataPageData(byte[] pageBytes);

    // 获取索引页内容
    byte[] getIndexPageData(byte[] pageBytes);

    // 修改页头信息
    byte[] updatePageHeader(byte[] pageBytes, PageManager.PageHeader newHeader);

    // 添加记录到页
    byte[] addRecordToPage(byte[] pageBytes, byte[] recordData);

    // 更新页中的记录
    byte[] updateRecordInPage(byte[] pageBytes, int slotIndex, byte[] newRecord);

    // 删除页中的记录
    byte[] deleteRecordFromPage(byte[] pageBytes, int slotIndex);

    // 获取页中的记录
    byte[] getRecordFromPage(byte[] pageBytes, int slotIndex);

    // 获取页头信息
    PageManager.PageHeader getPageHeader(byte[] pageBytes);

    // 获取空闲空间大小
    int getFreeSpace(byte[] pageBytes);*/
}
