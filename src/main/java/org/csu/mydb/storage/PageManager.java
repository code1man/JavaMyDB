package org.csu.mydb.storage;

import org.csu.mydb.storage.bufferPool.BufferPool;
import org.csu.mydb.storage.disk.DiskAccessor;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

//页管理系统
public class PageManager implements DiskAccessor {
    // ========================== 常量定义 ==========================
    public static final int PAGE_SIZE = 4096; // 4KB页大小
    //    public static final int BUFFER_POOL_SIZE = 100; // 缓存池大小
//    public static final int DEFAULT_FANOUT = 100; // B+树默认分支因子
    public static final int PAGE_HEADER_SIZE = 41; // 页头大小
    public static final int SLOT_SIZE = 6; // 槽位大小
//    public static final int SLOT_COUNT =100; // 默认一页中槽位数

    // 添加非静态 BufferPool 引用
    private BufferPool bufferPool;

    // 添加设置方法
    public void setBufferPool(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    // ========================== 核心数据结构 ==========================

    /**
     * 全局页标识符 (表空间ID + 页号)
     */
    public static class GlobalPageId {
        public final int spaceId; // 表空间ID
        public final int pageNo; // 页号

        public GlobalPageId(int spaceId, int pageNo) {
            this.spaceId = spaceId;
            this.pageNo = pageNo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GlobalPageId that = (GlobalPageId) o;
            return spaceId == that.spaceId && pageNo == that.pageNo;
        }

        @Override
        public int hashCode() {
            return Objects.hash(spaceId, pageNo);
        }

        public int getSpaceId() {
            return spaceId;
        }

        public int getPageNo() {
            return pageNo;
        }
    }

    /**
     * 页头元数据（24字节）
     */
    public static class PageHeader {
        public int pageNo;          // 4字节 - 页号
        public int prevPage;        // 4字节 - 上一页
        public int nextPage;        // 4字节 - 下一页
        public short recordCount;   // 2字节 - 记录数
        public short freeSpace;     // 2字节 - 空闲空间大小
        public short slotCount;     // 2字节 - 槽位数
        public short firstFreeSlot; // 2字节 - 第一个空闲槽位索引
        public short lastSlotOffset; // 2字节 - 最后一个槽位的相对页的偏移量（其实不怎么用上）
        public byte pageType;       // 1字节 - 页类型 (0=数据页, 1=索引页)
        public byte flags;          // 1字节 - 标志位
        public int checksum;        // 4字节 - 校验和
        public boolean isDirty;    // 1字节 - 是否为脏页
        public int rightPointer;   // 4字节 - 用于索引页，即b+树的最右侧指针
        public int nextFreePage;   // 4字节 - 用于空闲页，指向下一个空闲页
        public int nextFragPage;   // 4字节 - 用于碎片页，指向下一个碎片页

        /**
         * 序列化为字节数组
         */
        byte[] toBytes() {
            ByteBuffer buffer = ByteBuffer.allocate(PAGE_HEADER_SIZE);

            // 写入整型字段
            buffer.putInt(pageNo);
            buffer.putInt(prevPage);
            buffer.putInt(nextPage);

            // 写入短整型字段
            buffer.putShort(recordCount);
            buffer.putShort(freeSpace);
            buffer.putShort(slotCount);
            buffer.putShort(firstFreeSlot);
            buffer.putShort(lastSlotOffset);

            // 写入字节字段
            buffer.put(pageType);
            buffer.put(flags);

            // 写入校验和
            buffer.putInt(checksum);

            // 写入布尔字段
            buffer.put(isDirty ? (byte) 1 : (byte) 0);

            return buffer.array();
        }

        /**
         * 从字节数组反序列化
         */
        static PageHeader fromBytes(byte[] data) {
            if (data.length < PAGE_HEADER_SIZE) {
                throw new IllegalArgumentException("Invalid header data size");
            }

            ByteBuffer buffer = ByteBuffer.wrap(data);
            PageHeader header = new PageHeader();

            // 读取整型字段
            header.pageNo = buffer.getInt();
            header.prevPage = buffer.getInt();
            header.nextPage = buffer.getInt();

            // 读取短整型字段
            header.recordCount = buffer.getShort();
            header.freeSpace = buffer.getShort();
            header.slotCount = buffer.getShort();
            header.firstFreeSlot = buffer.getShort();
            header.lastSlotOffset = buffer.getShort();

            // 读取字节字段
            header.pageType = buffer.get();
            header.flags = buffer.get();

            // 读取校验和
            header.checksum = buffer.getInt();

            // 读取布尔字段
            header.isDirty = buffer.get() == 1;

            return header;
        }

        public int getPageNo() {
            return pageNo;
        }

        public int getPrevPage() {
            return prevPage;
        }

        public int getNextPage() {
            return nextPage;
        }

        public short getRecordCount() {
            return recordCount;
        }

        public short getFreeSpace() {
            return freeSpace;
        }

        public short getSlotCount() {
            return slotCount;
        }

        public short getFirstFreeSlot() {
            return firstFreeSlot;
        }

        public short getLastSlotOffset() {
            return lastSlotOffset;
        }

        public byte getPageType() {
            return pageType;
        }

        public byte getFlags() {
            return flags;
        }

        public int getChecksum() {
            return checksum;
        }

        public boolean isDirty() {
            return isDirty;
        }
    }

    /**
     * 槽位结构（6字节）
     */
    public static class Slot {
        short offset;   // 2字节 - 记录在数据区的偏移量
        short length;   // 2字节 - 记录长度
        byte status;    // 1字节 - 状态 (0=空闲, 1=使用中, 2=已删除)
        byte nextFree;  // 1字节 - 下一个空闲槽位索引（仅当status=0时有效）
        // 总计: 6字节

        // 序列化为字节
        byte[] toBytes() {
            ByteBuffer buffer = ByteBuffer.allocate(SLOT_SIZE);
            buffer.putShort(offset);
            buffer.putShort(length);
            buffer.put(status);
            buffer.put(nextFree);
            return buffer.array();
        }

        // 从字节反序列化
        static Slot fromBytes(byte[] data) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            Slot slot = new Slot();
            slot.offset = buffer.getShort();
            slot.length = buffer.getShort();
            slot.status = buffer.get();
            slot.nextFree = buffer.get();
            return slot;
        }

        public short getOffset() {
            return offset;
        }

        public short getLength() {
            return length;
        }

        public byte getStatus() {
            return status;
        }

        public byte getNextFree() {
            return nextFree;
        }
    }

    /**
     * 数据页
     * 我想把他抽象成页
     */
    public static class Page {
        PageHeader header;

        //每个记录的位置
        List<Slot> slots = new ArrayList<>();
        byte[] pageData; // 整个页的原始数据

        // 槽位数组起始位置
        int slotsStartOffset = PAGE_HEADER_SIZE;

        // 数据区起始位置（从页尾开始）
        int dataStartOffset = PAGE_SIZE;

        public Page(int pageNo) {
            this.header = new PageHeader();
            this.header.pageNo = pageNo;
            this.header.prevPage = -1;
            this.header.nextPage = -1;
            this.header.recordCount = 0;
            this.header.slotCount = 0;
            this.header.firstFreeSlot = -1; //因为还没新建槽位
            this.header.lastSlotOffset = -1; //因为还没新建槽位
            this.header.freeSpace = PAGE_SIZE - PAGE_HEADER_SIZE; // 初始空闲空间
//            this.header.pageType = 0; // 先不设置类型
            this.header.isDirty = true; // 新建一个页需要保存到磁盘
            this.header.rightPointer = -1; // 没有最右指针
            this.header.nextFreePage = -1;//初始化
            this.header.nextFragPage = -1;//初始化
            this.pageData = new byte[PAGE_SIZE];

            /*// 计算槽位数组占用空间
            int slotsAreaSize = calculateSlotsAreaSize();

            // 初始化数据区（总空间减去页头和槽位数组）
            this.data = new byte[PAGE_SIZE - PAGE_HEADER_SIZE - slotsAreaSize];

            // 设置空闲空间（初始时只有数据区可用）
            this.header.freeSpace = (short) data.length;

            // 初始化槽位
            initSlots(slotsAreaSize / SLOT_SIZE);*/
        }

        /**
         * 往数据页中添加记录
         */
        public boolean addRecord(byte[] record) {
            int recordSize = record.length;

            // 所需空间
            int requiredSpace;

            //先判断有无空闲槽位
            // 检查是否需要新增槽位
            boolean needNewSlot = (header.firstFreeSlot == -1);

            //如果需要
            if (needNewSlot) {
                // 所需空间 = 记录大小 + 新槽位大小
                requiredSpace = recordSize + SLOT_SIZE;

                // 检查剩余空间是否足够(还要进行下面的判断，页的管理可能存在碎片化)
                if (header.freeSpace < requiredSpace) {
                    return false;
                }

                // 检查是否有空间添加新槽位+数据
                int newSlotEnd = slotsStartOffset + (slots.size() + 1) * SLOT_SIZE;
                if (newSlotEnd > dataStartOffset - recordSize) {
                    return false; // 空间不足（槽位数组和数据区相遇）
                }
            }   //0 1 | 2 3 4 5 | 6 7 8 9
            else {
                requiredSpace = recordSize;

                // 检查剩余空间是否足够(还要进行下面的判断，页的管理可能存在碎片化)
                if (header.freeSpace < requiredSpace) {
                    return false;
                }

                // 检查是否有空间添加数据
                int slotEnd = slotsStartOffset + slots.size() * SLOT_SIZE;
                if (slotEnd > dataStartOffset - recordSize) {
                    return false; // 空间不足（槽位数组和数据区相遇）
                }
            }
            //0 1 | 2 3 4 5 | 6 7 8 9

            // 分配槽位
            int slotIndex;
            if (!needNewSlot) {
                // 重用空闲槽位
                slotIndex = header.firstFreeSlot;
                Slot slot = slots.get(slotIndex);
                //相当于更新空闲链表
                header.firstFreeSlot = slot.nextFree;

                // 更新槽位信息
                slot.offset = (short) (dataStartOffset - recordSize);
                slot.length = (short) recordSize;
                slot.status = 1;

            } else {
                // 添加新槽位，上面已经验证过空间够了
                slotIndex = slots.size();
                Slot slot = new Slot();
                slot.offset = (short) (dataStartOffset - recordSize);
                slot.length = (short) recordSize;
                slot.status = 1;
                slots.add(slot);
                header.slotCount++;

                //更新页头中的最后一个槽位偏移信息
                header.lastSlotOffset = (short) (slotsStartOffset + (slots.size() - 1) * SLOT_SIZE);
            }

            // 写入记录数据
            System.arraycopy(record, 0, pageData, dataStartOffset - recordSize, recordSize);

            dataStartOffset -= recordSize;

            // 更新页头
            header.recordCount += 1;
            header.freeSpace -= requiredSpace;
            header.isDirty = true;


            //写入槽位数据
            int slotsOffset = PAGE_HEADER_SIZE;
            for (Slot slot : slots) {
                byte[] slotBytes = slot.toBytes();
                System.arraycopy(slotBytes, 0, pageData, slotsOffset, SLOT_SIZE);
                slotsOffset += SLOT_SIZE;
            }
            //写入页头数据
            System.arraycopy(header.toBytes(), 0, pageData, 0, PAGE_HEADER_SIZE);

            return true;
        }

        /**
         * 获取记录
         */
        public byte[] getRecord(int slotIndex) {
            if (slotIndex < 0 || slotIndex >= slots.size()) {
                return null;
            }

            Slot slot = slots.get(slotIndex);
            if (slot.status != 1) {
                return null;
            }

            return Arrays.copyOfRange(pageData, slot.offset, slot.offset + slot.length);
        }

        /**
         * 释放记录
         * 数据为逻辑删除，但是槽位放出来
         * 考虑空闲链表的维护
         */
        public boolean freeRecord(int slotIndex) {
            if (slotIndex < 0 || slotIndex >= slots.size()) {
                return false;
            }

            Slot slot = slots.get(slotIndex);
            if (slot.status != 1) {
                return false;
            }

            //修改槽位信息
            // 标记为已删除
            slot.status = 0;  //或者改称2
            //它指向的数据的偏移量暂时不改，等待下次使用

            // 添加到空闲链表头部
            slot.nextFree = (byte) header.firstFreeSlot;
            header.firstFreeSlot = (short) slotIndex;

            // 更新页头
            header.recordCount--;  //逻辑上的记录数量
            header.isDirty = true;  //页修改了

            return true;
        }



        /**
         * 修改记录
         * 覆盖：但是记录的长度可能不一样
         * or新增记录：要考虑空间够不够的问题
         *
         * @param slotIndex 要修改的记录槽位索引
         * @param newRecord 新记录数据
         * @return 修改是否成功
         */
        public boolean updateRecord(int slotIndex, byte[] newRecord) {
            if (slotIndex < 0 || slotIndex >= slots.size()) {
                return false; // 无效槽位索引
            }

            Slot slot = slots.get(slotIndex);
            if (slot.status != 1) { // 确保槽位当前是使用状态
                return false;
            }

            int oldLength = slot.length;
            int newLength = newRecord.length;

            // 情况1：新记录长度 <= 旧记录长度
            if (newLength <= oldLength) {
                // 直接在原位置覆盖
                System.arraycopy(newRecord, 0, pageData, slot.offset, newLength);

                // 更新槽位信息
                slot.length = (short) newLength;

                // 更新空闲空间（如果新记录更短）
                if (newLength < oldLength) {
                    header.freeSpace += (oldLength - newLength);
                }

                header.isDirty = true;
                return true;
            }

            // 情况2：新记录长度 > 旧记录长度
            // 检查是否有足够空间容纳增长部分
            int spaceNeeded = newLength - oldLength;
            if (header.freeSpace < spaceNeeded) {
                return false; // 空间不足
            }

            // 尝试在数据区尾部分配新空间
            int newOffset = dataStartOffset - newLength;
            if (newOffset < (slotsStartOffset + slots.size() * SLOT_SIZE)) {
                // 空间不足（槽位数组和数据区重叠）
                return false;
            }

            // 迁移记录到新位置
//            System.arraycopy(data, slot.offset, data, newOffset, oldLength); // 复制旧数据
            System.arraycopy(newRecord, 0, pageData, newOffset, newLength);    // 覆盖新数据

            // 更新槽位信息
            slot.offset = (short) newOffset;
            slot.length = (short) newLength;

            // 释放旧空间
            header.freeSpace += oldLength; // 释放旧空间
            header.freeSpace -= newLength; // 占用新空间

            // 更新数据区起始位置
            dataStartOffset = newOffset;

            header.isDirty = true;
            return true;
        }

        /**
         * 序列化整个数据页
         * 原本这个记录就是序列化之后存进去的
         * 额外序列化一下页头和槽位数组
         */
        public byte[] toBytes() {
            // 1. 序列化页头
            byte[] headerBytes = header.toBytes();
            System.arraycopy(headerBytes, 0, pageData, 0, PAGE_HEADER_SIZE);

            // 2. 序列化槽位数组
            int slotsOffset = PAGE_HEADER_SIZE;
            for (Slot slot : slots) {
                byte[] slotBytes = slot.toBytes();
                System.arraycopy(slotBytes, 0, pageData, slotsOffset, SLOT_SIZE);
                slotsOffset += SLOT_SIZE;
            }

            // 3. 填充槽位数组后的空闲区域（如果有）
            int slotsEnd = PAGE_HEADER_SIZE + slots.size() * SLOT_SIZE;
            if (slotsEnd < dataStartOffset) {
                Arrays.fill(pageData, slotsEnd, dataStartOffset, (byte) 0);
            }

            // 4. 数据区已经在pageData中（在添加记录时已写入）
            // 注意：数据区从dataStartOffset开始到PAGE_SIZE结束

            return pageData;
        }

        /**
         * 从字节数组反序列化数据页
         */
        public static Page fromBytes(byte[] pageData) {
            if (pageData.length != PAGE_SIZE) {
                throw new IllegalArgumentException("Invalid page size");
            }

            // 1. 反序列化页头
            byte[] headerData = Arrays.copyOfRange(pageData, 0, PAGE_HEADER_SIZE);
            PageHeader pageHeader = PageHeader.fromBytes(headerData);

            //先从页头中获得页号
            Page page = new Page(pageHeader.pageNo);
            System.arraycopy(pageData, 0, page.pageData, 0, PAGE_SIZE);
            page.header = pageHeader;

            // 2. 反序列化槽位数组
            int slotsStart = PAGE_HEADER_SIZE;
            int slotsEnd = slotsStart + page.header.slotCount * SLOT_SIZE;

            for (int i = 0; i < page.header.slotCount; i++) {
                int slotStart = slotsStart + i * SLOT_SIZE;
                byte[] slotData = Arrays.copyOfRange(pageData, slotStart, slotStart + SLOT_SIZE);
                Slot slot = Slot.fromBytes(slotData);
                page.slots.add(slot);
            }

            // 3. 确定数据区起始位置
            // 查找最小的槽位偏移量
            int minOffset = PAGE_SIZE;
            for (Slot slot : page.slots) {
                if (slot.status == 1 && slot.offset < minOffset) {
                    minOffset = slot.offset;
                }
            }
            page.dataStartOffset = minOffset;

            return page;
        }

        public PageHeader getHeader() {
            return header;
        }

        public List<Slot> getSlots() {
            return slots;
        }

        public byte[] getPageData() {
            return pageData;
        }

        public int getSlotsStartOffset() {
            return slotsStartOffset;
        }

        public int getDataStartOffset() {
            return dataStartOffset;
        }
    }

    // ====================== 文件管理 ======================
    private static final Map<Integer, RandomAccessFile> openFiles = new HashMap<>();
    private static final Map<Integer, String> filePaths = new HashMap<>();
    private static final ReentrantReadWriteLock fileLock = new ReentrantReadWriteLock();

    // ====================== 页管理 ======================
    private static final Map<Integer, Integer> rootPages = new HashMap<>(); // spaceId -> rootPageNo
    private static final Map<Integer, Integer> freePageHeads = new HashMap<>(); // spaceId -> freePageHead

    public Map<Integer, RandomAccessFile> getOpenFiles() {
        return openFiles;
    }

    public Map<Integer, String> getFilePaths() {
        return filePaths;
    }

    public ReentrantReadWriteLock getFileLock() {
        return fileLock;
    }

    public Map<Integer, Integer> getRootPages() {
        return rootPages;
    }

    public Map<Integer, Integer> getFreePageHeads() {
        return freePageHeads;
    }

    /**
     * 打开表空间文件（不管新建表，读表都要）
     * 相当于保存某个文件到    内存里面
     */
    public void openFile(int spaceId, String filePath) throws IOException {
        fileLock.writeLock().lock();
        try {
            File file = new File(filePath);
            RandomAccessFile raf;

            if (file.exists()) {
                raf = new RandomAccessFile(file, "rw");

                //这里应该先放进去
                openFiles.put(spaceId, raf);

                //这里应该先看缓存---------------------------------------------------------------------
                //这里需要修改一下
                Page headerPage = getPage(spaceId,0);
                //缓存里面已经包含看磁盘了

                freePageHeads.put(spaceId, headerPage.header.nextPage);
            } else {
                raf = new RandomAccessFile(file, "rw");
                // 初始化文件
                initializeFile(spaceId, raf);
                openFiles.put(spaceId, raf);
            }

            filePaths.put(spaceId, filePath);
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * 关闭所有文件
     * 内存中删除文件
     */
    public void closeAllFiles() throws IOException {
        fileLock.writeLock().lock();
        try {
//            flushAllDirtyPages();
            for (RandomAccessFile raf : openFiles.values()) {
                raf.close();
            }
            openFiles.clear();
            filePaths.clear();
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * 分配新页
     */
    public int allocatePage(int spaceId) throws IOException {
        fileLock.writeLock().lock();
        try {
            // 检查空闲页链表
            if (freePageHeads.containsKey(spaceId) && freePageHeads.get(spaceId) != -1) {
                int pageNo = freePageHeads.get(spaceId);
                Page page = getPage(spaceId, pageNo);
                freePageHeads.put(spaceId, page.header.nextPage);
                return pageNo;
            }

            // 没有空闲页，扩展文件
            RandomAccessFile raf = openFiles.get(spaceId);
            long fileSize = raf.length();
            int newPageNo = (int) (fileSize / PAGE_SIZE);

            // 扩展文件
            raf.setLength(fileSize + PAGE_SIZE);

            // 初始化新页
            Page newPage = new Page(newPageNo);

            //这里应该先放到缓存里面--------------------------------------------------------
            //设置成dirty
//            writePageToDisk(spaceId, newPageNo, newPage);
            bufferPool.putPage(newPage, spaceId);

            return newPageNo;
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * 释放页
     */
    public void freePage(int spaceId, int pageNo) throws IOException {
        fileLock.writeLock().lock();
        try {
            Page page = getPage(spaceId, pageNo);

            // 重置页内容
            page.header.recordCount = 0;
            page.header.freeSpace = (short) (PAGE_SIZE - PAGE_HEADER_SIZE);

            // 添加到空闲链表头部
            int currentHead = freePageHeads.getOrDefault(spaceId, -1);
            page.header.nextPage = currentHead;
            freePageHeads.put(spaceId, pageNo);

            // 更新文件头页
            Page headerPage = getPage(spaceId, 0);
            headerPage.header.nextPage = pageNo;
            headerPage.header.isDirty = true;

            //清除的页也要标记成脏页
            bufferPool.putPage(page, spaceId);
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    /**
     * 添加记录
     */
    public boolean addRecord(int spaceId, int pageNo, byte[] record) throws IOException {
        Page page = getPage(spaceId, pageNo);
        boolean success = page.addRecord(record);
//        if (success) {
//            markPageDirty(spaceId, pageNo);
//        }

        //放到缓存里面--------------------------------------------------------
        bufferPool.putPage(page, spaceId);

        return success;
    }

    /**
     * 获取记录
     */
    public byte[] getRecord(int spaceId, int pageNo, int slotIndex) throws IOException {
        Page page = getPage(spaceId, pageNo);
        return page.getRecord(slotIndex);
    }

    /**
     * 释放记录
     */
    public boolean freeRecord(int spaceId, int pageNo, int slotIndex) throws IOException {
        Page page = getPage(spaceId, pageNo);
        boolean success = page.freeRecord(slotIndex);
//        if (success) {
//            markPageDirty(spaceId, pageNo);
//        }
        bufferPool.putPage(page, spaceId);

        return success;
    }

    /**
     * 修改记录
     */
    public boolean updateRecord(int spaceId, int pageNo, int slotIndex, byte[] newRecord) throws IOException {
        Page page = getPage(spaceId, pageNo);
        boolean success = page.updateRecord(slotIndex, newRecord);
//        if (success) {
//            markPageDirty(spaceId, pageNo);
//        }
        bufferPool.putPage(page, spaceId);

        return success;
    }

    // ====================== 缓存管理 ======================

    /**
     * 获取页（带缓存）
     */
    public Page getPage(int spaceId, int pageNo) throws IOException {
        GlobalPageId pageId = new GlobalPageId(spaceId, pageNo);

//        // 检查缓存
//        if (bufferPool.containsKey(pageId)) {
//            // 更新LRU队列
//            synchronized (lruQueue) {
//                lruQueue.remove(pageId);
//                lruQueue.addFirst(pageId);
//            }
//            return bufferPool.get(pageId);
//        }

        // 缓存未命中，从磁盘读取
//        DataPage page = readPageFromDisk(spaceId, pageNo);

//        // 如果缓存已满，淘汰最近最少使用的页
//        if (bufferPool.size() >= BUFFER_POOL_SIZE) {
//            synchronized (lruQueue) {
//                GlobalPageId evictedId = lruQueue.removeLast();
//                DataPage evictedPage = bufferPool.remove(evictedId);
//
//                // 如果是脏页，写回磁盘
//                if (evictedPage.header.isDirty) {
//                    writePageToDisk(
//                            evictedId.spaceId,
//                            evictedId.pageNo,
//                            evictedPage
//                    );
//                }
//            }
//        }
//
//        // 添加新页到缓存
//        bufferPool.put(pageId, page);
//        synchronized (lruQueue) {
//            lruQueue.addFirst(pageId);
//        }

        return bufferPool.getPage(pageId);
    }

//    /**
//     * 标记页为脏页
//     */
//    private void markPageDirty(int spaceId, int pageNo) {
//        GlobalPageId pageId = new GlobalPageId(spaceId, pageNo);
//        DataPage page = bufferPool.get(pageId);
//        if (page != null) {
//            page.header.isDirty = true;
//        }
//    }
//
//    /**
//     * 刷回所有脏页
//     */
//    public void flushAllDirtyPages() throws IOException {
//        fileLock.writeLock().lock();
//        try {
//            for (Map.Entry<GlobalPageId, DataPage> entry : bufferPool.entrySet()) {
//                if (entry.getValue().header.isDirty) {
//                    writePageToDisk(
//                            entry.getKey().spaceId,
//                            entry.getKey().pageNo,
//                            entry.getValue()
//                    );
//                    entry.getValue().header.isDirty = false;
//                }
//            }
//        } finally {
//            fileLock.writeLock().unlock();
//        }
//    }

    // ====================== 私有辅助方法 ======================

    /**
     * 初始化文件
     */
    private void initializeFile(int spaceId, RandomAccessFile raf) throws IOException {
        // 创建文件头页
        Page headerPage = new Page(0);
        headerPage.header.nextPage = -1; // 初始无空闲页
        raf.write(headerPage.toBytes());

        // 设置空闲页链表头
        freePageHeads.put(spaceId, -1);
    }

    /**
     * 从磁盘读取页
     */
    public Page readPageFromDisk(int spaceId, int pageNo) throws IOException {
        fileLock.readLock().lock();
        try {
            RandomAccessFile raf = openFiles.get(spaceId);
            if (raf == null) {
                throw new IOException("Space not found: " + spaceId);
            }

            long offset = (long) pageNo * PAGE_SIZE;
            raf.seek(offset);

            byte[] pageData = new byte[PAGE_SIZE];
            raf.readFully(pageData);

            Page page = Page.fromBytes(pageData);

            //读完磁盘后记得要放入缓存

            return page;
        } finally {
            fileLock.readLock().unlock();
        }
    }

    /**
     * 写入页到磁盘
     */
    public void writePageToDisk(int spaceId, int pageNo, Page page) throws IOException {
        fileLock.writeLock().lock();
        try {
            RandomAccessFile raf = openFiles.get(spaceId);
            if (raf == null) {
                throw new IOException("Space not found: " + spaceId);
            }

            long offset = (long) pageNo * PAGE_SIZE;
            raf.seek(offset);
            raf.write(page.toBytes());
        } finally {
            fileLock.writeLock().unlock();
        }
    }

    // 写入磁盘(弃用)
    public static void writePage(Page page, int spaceId) throws IOException {
    }

    //从磁盘里面获取(弃用)
    public static Page readPage(GlobalPageId pageId) throws IOException {
        return null;
    }
}
