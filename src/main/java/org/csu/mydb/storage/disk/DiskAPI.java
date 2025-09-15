//package org.csu.mydb.storage.disk;
//
//import org.csu.mydb.storage.PageManager;
//
//import java.io.IOException;
//import java.io.RandomAccessFile;
//
//import static org.csu.mydb.storage.PageManager.fileLock;
//
//public class DiskAPI implements DiskAccessor{
//    public PageManager.DataPage readPageFromDisk(int spaceId, int pageNo) throws IOException {
//        fileLock.readLock().lock();
//        try {
//            RandomAccessFile raf = openFiles.get(spaceId);
//            if (raf == null) {
//                throw new IOException("Space not found: " + spaceId);
//            }
//
//            long offset = (long) pageNo * PAGE_SIZE;
//            raf.seek(offset);
//
//            byte[] pageData = new byte[PAGE_SIZE];
//            raf.readFully(pageData);
//
//            PageManager.DataPage page = PageManager.DataPage.fromBytes(pageData);
//
//            //读完磁盘后记得要放入缓存
//
//            return page;
//        } finally {
//            fileLock.readLock().unlock();
//        }
//    }
//
//    /**
//     * 写入页到磁盘
//     */
//    public void writePageToDisk(int spaceId, int pageNo, PageManager.DataPage page) throws IOException {
//        fileLock.writeLock().lock();
//        try {
//            RandomAccessFile raf = openFiles.get(spaceId);
//            if (raf == null) {
//                throw new IOException("Space not found: " + spaceId);
//            }
//
//            long offset = (long) pageNo * PAGE_SIZE;
//            raf.seek(offset);
//            raf.write(page.toBytes());
//        } finally {
//            fileLock.writeLock().unlock();
//        }
//    }
//}
