package org.csu.mydb.storage.storageFiles.page;


import org.csu.mydb.storage.PageManager;

//索引页
public class IndexPage extends PageManager.Page{

    public IndexPage(int pageNo) {
        super(pageNo);
        //设置成索引页
        this.getHeader().pageType = 1;
    }
}
