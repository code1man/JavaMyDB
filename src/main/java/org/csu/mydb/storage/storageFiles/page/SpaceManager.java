package org.csu.mydb.storage.storageFiles.page;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.bufferPool.BufferPool;
import org.csu.mydb.storage.storageFiles.FileHeader;

import java.io.IOException;
import java.nio.ByteBuffer;

//管理空闲链表和碎片链表
public class SpaceManager {
    private final PageManager pageManager;
    private final BufferPool bufferPool;

    public SpaceManager(PageManager pageManager, BufferPool bufferPool) {
        this.pageManager = pageManager;
        this.bufferPool = bufferPool;
    }

    /**
     * 维护空闲链表
     * @param spaceId
     * @throws IOException
     */
    public void maintainSpaceChains(int spaceId) throws IOException {
        // 1. 获取文件头页
        PageManager.Page headerPage = pageManager.getPage(spaceId, 0);

        // 2. 读取文件头信息
        int totalPage = ByteBuffer.wrap(headerPage.getRecord(1)).getInt();
        int currentFreeHead = ByteBuffer.wrap(headerPage.getRecord(2)).getInt();

        // 3. 初始化空闲链表
        int newFreeHead = -1;
        int prevFreePage = -1;

        // 4. 遍历所有页
        for (int pageNo = 3; pageNo < totalPage; pageNo++) {
            PageManager.Page page = pageManager.getPage(spaceId, pageNo);

            // 5. 检查页是否空闲
            if (isPageBecomeFree(page)) {
                // 6. 更新页头
                page.header.nextFreePage = -1; // 初始化为链表尾

                // 7. 添加到空闲链表
                if (newFreeHead == -1) {
                    // 第一个空闲页
                    newFreeHead = pageNo;
                } else {
                    // 链接到前一个空闲页
                    PageManager.Page prevPage = pageManager.getPage(spaceId, prevFreePage);
                    prevPage.header.nextFreePage = pageNo;
                    bufferPool.putPage(prevPage, spaceId);
                }

                // 8. 更新前一个空闲页指针
                prevFreePage = pageNo;

                // 9. 标记页为脏页
                bufferPool.putPage(page, spaceId);
            }
        }

        // 10. 更新文件头
        if (newFreeHead != -1) {
            // 更新文件头记录
            ByteBuffer buffer = ByteBuffer.allocate(4);
            buffer.putInt(newFreeHead);
            headerPage.updateRecord(2, buffer.array());

            // 标记文件头为脏页
            bufferPool.putPage(headerPage, spaceId);

            //内存记得改
            pageManager.getFreePageHeads().put(spaceId, newFreeHead);
        }
    }
//    /**
//     * 维护空闲链表和碎片链表
//     * @param spaceId 表空间ID
//     * @param modifiedPageNo 被修改的页号
//     */
//    public void maintainSpaceChains(int spaceId, int modifiedPageNo) throws IOException {
//        // 1. 获取文件头页
//        PageManager.Page headerPage = pageManager.getPage(spaceId, 0);
//        FileHeader fileHeader = parseFileHeader(headerPage);
//
//        // 2. 获取被修改的页
//        PageManager.Page modifiedPage = pageManager.getPage(spaceId, modifiedPageNo);
//
//        // 3. 根据页状态更新链表
//        if (isPageBecomeFree(modifiedPage)) {
//            addToFreeList(spaceId, modifiedPageNo, fileHeader.freePageHead);
//            fileHeader.freePageHead = modifiedPageNo;
//        } else if (isPageBecomeFragmented(modifiedPage)) {
//            addToFragList(spaceId, modifiedPageNo, fileHeader.fragPageHead);
//            fileHeader.fragPageHead = modifiedPageNo;
//        } else if (isPageReclaimed(modifiedPage)) {
//            removeFromChains(spaceId, modifiedPageNo, fileHeader);
//        }
//
//        // 4. 更新文件头
//        updateFileHeader(headerPage, fileHeader);
//    }
//
    /**
     * 判断页是否变为空闲页
     */
    private boolean isPageBecomeFree(PageManager.Page page) {
        // 条件1: 页内所有记录都被删除

        return page.header.recordCount == 0 &&
        page.header.freeSpace >= PageManager.PAGE_SIZE * 0.9; // 90%以上空闲;
    }

    /**
     * 判断页是否变为碎片页
     */
//    private boolean isPageBecomeFragmented(PageManager.Page page) {
//        // 条件1: 页利用率低于阈值
//        // 条件2: 页不是系统页
//        float utilization = (float) (PageManager.PAGE_SIZE - page.header.freeSpace) / PageManager.PAGE_SIZE;
//        return utilization < 0.3f &&
//                page.header.pageType != PageType.SYSTEM_PAGE;
//    }
//
//    /**
//     * 判断页是否被重新利用
//     */
//    private boolean isPageReclaimed(PageManager.Page page) {
//        // 条件: 页从空闲/碎片状态变为使用状态
//        return (page.header.pageType == PageType.FREE_PAGE ||
//                page.header.pageType == PageType.FRAG_PAGE) &&
//                (page.header.pageType != page.header.prevPageType);
//    }
//
//    /**
//     * 将页添加到空闲链表
//     */
//    private void addToFreeList(int spaceId, int pageNo, int currentFreeHead) throws IOException {
//        // 1. 获取页
//        PageManager.Page page = pageManager.getPage(spaceId, pageNo);
//
//        // 2. 更新页头
//        page.header.prevPageType = page.header.pageType; // 保存前一个状态
//        page.header.pageType = PageType.FREE_PAGE;
//        page.header.nextFreePage = currentFreeHead; // 指向当前链表头
//        page.header.isDirty = true;
//
//        // 3. 保存到缓存
//        bufferPool.putPage(page, spaceId);
//    }
//
//    /**
//     * 将页添加到碎片链表
//     */
//    private void addToFragList(int spaceId, int pageNo, int currentFragHead) throws IOException {
//        // 1. 获取页
//        PageManager.Page page = pageManager.getPage(spaceId, pageNo);
//
//        // 2. 更新页头
//        page.header.prevPageType = page.header.pageType; // 保存前一个状态
//        page.header.pageType = PageType.FRAG_PAGE;
//        page.header.nextFragPage = currentFragHead; // 指向当前链表头
//        page.header.isDirty = true;
//
//        // 3. 保存到缓存
//        bufferPool.putPage(page, spaceId);
//    }
//
//    /**
//     * 从链表中移除页
//     */
//    private void removeFromChains(int spaceId, int pageNo, FileHeader fileHeader) throws IOException {
//        // 1. 获取页
//        PageManager.Page page = pageManager.getPage(spaceId, pageNo);
//
//        // 2. 从空闲链表移除
//        if (page.header.prevPageType == PageType.FREE_PAGE) {
//            removeFromFreeList(spaceId, pageNo, fileHeader);
//        }
//        // 3. 从碎片链表移除
//        else if (page.header.prevPageType == PageType.FRAG_PAGE) {
//            removeFromFragList(spaceId, pageNo, fileHeader);
//        }
//
//        // 4. 清除链表标记
//        page.header.prevPageType = page.header.pageType;
//        page.header.isDirty = true;
//
//        // 5. 保存到缓存
//        bufferPool.putPage(page, spaceId);
//    }
//
//    /**
//     * 从空闲链表移除页
//     */
//    private void removeFromFreeList(int spaceId, int pageNo, FileHeader fileHeader) throws IOException {
//        // 1. 如果移除的是链表头
//        if (fileHeader.freePageHead == pageNo) {
//            PageManager.Page page = pageManager.getPage(spaceId, pageNo);
//            fileHeader.freePageHead = page.header.nextFreePage;
//        } else {
//            // 2. 遍历链表找到前驱页
//            int prevPage = findPreviousPage(spaceId, pageNo, fileHeader.freePageHead, true);
//            if (prevPage != -1) {
//                PageManager.Page prev = pageManager.getPage(spaceId, prevPage);
//                prev.header.nextFreePage = pageManager.getPage(spaceId, pageNo).header.nextFreePage;
//                prev.header.isDirty = true;
//                bufferPool.putPage(prev, spaceId);
//            }
//        }
//    }
//
//    /**
//     * 从碎片链表移除页
//     */
//    private void removeFromFragList(int spaceId, int pageNo, FileHeader fileHeader) throws IOException {
//        // 1. 如果移除的是链表头
//        if (fileHeader.fragPageHead == pageNo) {
//            PageManager.Page page = pageManager.getPage(spaceId, pageNo);
//            fileHeader.fragPageHead = page.header.nextFragPage;
//        } else {
//            // 2. 遍历链表找到前驱页
//            int prevPage = findPreviousPage(spaceId, pageNo, fileHeader.fragPageHead, false);
//            if (prevPage != -1) {
//                PageManager.Page prev = pageManager.getPage(spaceId, prevPage);
//                prev.header.nextFragPage = pageManager.getPage(spaceId, pageNo).header.nextFragPage;
//                prev.header.isDirty = true;
//                bufferPool.putPage(prev, spaceId);
//            }
//        }
//    }
//
//    /**
//     * 查找链表中的前驱页
//     */
//    private int findPreviousPage(int spaceId, int targetPage, int startPage, boolean isFreeList) throws IOException {
//        int current = startPage;
//        int prev = -1;
//
//        while (current != -1) {
//            PageManager.Page page = pageManager.getPage(spaceId, current);
//            int nextPage = isFreeList ? page.header.nextFreePage : page.header.nextFragPage;
//
//            if (nextPage == targetPage) {
//                return current;
//            }
//
//            prev = current;
//            current = nextPage;
//        }
//
//        return -1;
//    }
//
//    /**
//     * 更新文件头页
//     */
//    private void updateFileHeader(PageManager.Page headerPage, FileHeader fileHeader) {
//        // 1. 更新记录
//        headerPage.updateRecord(0, fileHeader.toBytes());
//        headerPage.header.isDirty = true;
//
//        // 2. 保存到缓存
//        bufferPool.putPage(headerPage, headerPage.header.spaceId);
//    }
//
//    /**
//     * 解析文件头
//     */
//    private FileHeader parseFileHeader(PageManager.Page headerPage) {
//        byte[] headerData = headerPage.getRecord(0);
//        return FileHeader.fromBytes(headerData);
//    }
}
