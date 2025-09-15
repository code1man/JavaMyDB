package org.csu.mydb.storage.storageFiles;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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

    //每个部分分别转成byte[]
    public List<byte[]> toBytesList() {
        List<byte[]> list = new ArrayList<>();

        ByteBuffer buffer1 = ByteBuffer.allocate(4);
        ByteBuffer buffer2 = ByteBuffer.allocate(4);
        ByteBuffer buffer3 = ByteBuffer.allocate(4);
        ByteBuffer buffer4 = ByteBuffer.allocate(4);

        buffer1.putInt(spaceId);
        buffer2.putInt(pageCount);
        buffer3.putInt(firstFreePage);
        buffer4.putInt(firstFragPage);

        list.add(buffer1.array());
        list.add(buffer2.array());
        list.add(buffer3.array());
        list.add(buffer4.array());

        return list;
    }
}
