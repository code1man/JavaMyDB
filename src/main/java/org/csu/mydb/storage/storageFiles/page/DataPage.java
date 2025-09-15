package org.csu.mydb.storage.storageFiles.page;

import org.csu.mydb.storage.PageManager;

//数据页
public class DataPage extends PageManager.Page {
    public DataPage(int pageNo) {
        super(pageNo);
        this.getHeader().pageType = 1;
    }
}
