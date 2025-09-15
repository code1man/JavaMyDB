package org.csu.mydb.storage.storageFiles;

//每个表空间的第0页（即页0）存的数据
public class FileHeader {

    //这个文件的表空间id
    private int spaceId;

    //页数
    private int pageCount;

    //空闲页链表头
    private int firstFreePage;

    //碎片页链表头
    private int firstFragPage;

    public FileHeader(int spaceId, int pageCount, int firstFreePage, int firstFragPage) {
        this.spaceId = spaceId;
        this.pageCount = pageCount;
        this.firstFreePage = firstFreePage;
        this.firstFragPage = firstFragPage;
    }

    public int getSpaceId() {
        return spaceId;
    }

    public void setSpaceId(int spaceId) {
        this.spaceId = spaceId;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    public int getFirstFreePage() {
        return firstFreePage;
    }

    public void setFirstFreePage(int firstFreePage) {
        this.firstFreePage = firstFreePage;
    }

    public int getFirstFragPage() {
        return firstFragPage;
    }

    public void setFirstFragPage(int firstFragPage) {
        this.firstFragPage = firstFragPage;
    }
}
