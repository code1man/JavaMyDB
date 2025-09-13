package org.csu.mydb.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

//页管理系统
public class PageManager {
    // ========================== 常量定义 ==========================
    public static int PAGE_SIZE = 4096; // 4KB页大小
    //    public static final int BUFFER_POOL_SIZE = 100; // 缓存池大小
    //    public static final int DEFAULT_FANOUT = 100; // B+树默认分支因子
    public static final int PAGE_HEADER_SIZE = 29; // 页头大小
    public static final int SLOT_SIZE = 6; // 槽位大小
    //    public static final int SLOT_COUNT =100; // 默认一页中槽位数

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
        public short lastSlotOffset; // 2字节 - 最后一个槽位的相对页的偏移量
        public byte pageType;       // 1字节 - 页类型 (0=数据页, 1=索引页)
        public byte flags;          // 1字节 - 标志位
        public int checksum;        // 4字节 - 校验和
        public boolean isDirty;    // 1字节 - 是否为脏页


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
    }

    /**
     * 数据页
     */
    public static class DataPage {
        PageHeader header;

        //每个记录的位置
        List<Slot> slots = new ArrayList<>();
        byte[] pageData; // 整个页的原始数据

        // 槽位数组起始位置
        int slotsStartOffset = PAGE_HEADER_SIZE;

        // 数据区起始位置（从页尾开始）
        int dataStartOffset = PAGE_SIZE;

        public DataPage(int pageNo) {
            this.header = new PageHeader();
            this.header.pageNo = pageNo;
            this.header.prevPage = -1;
            this.header.nextPage = -1;
            this.header.recordCount = 0;
            this.header.slotCount = 0;
            this.header.firstFreeSlot = -1; //因为还没新建槽位
            this.header.lastSlotOffset = -1; //因为还没新建槽位
            this.header.freeSpace = (short) (PAGE_SIZE - PAGE_HEADER_SIZE); // 初始空闲空间
            this.header.pageType = 0; // 数据页
            this.header.isDirty = true; // 新建一个页需要保存到磁盘
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
         * 添加记录
         */
        public boolean addRecord(byte[] record) {
            int recordSize = record.length;

            // 所需空间 = 记录大小 + 新槽位大小
            int requiredSpace = recordSize + SLOT_SIZE;

            // 检查剩余空间是否足够(还要进行下面的判断，页的管理可能存在碎片化)
            if (header.freeSpace < requiredSpace) {
                return false;
            }

            // 检查是否需要新增槽位
            boolean needNewSlot = (header.firstFreeSlot == -1);

            if (needNewSlot) {
                // 检查是否有空间添加新槽位
                int newSlotEnd = slotsStartOffset + (slots.size() + 1) * SLOT_SIZE;
                if (newSlotEnd > dataStartOffset - recordSize) {
                    return false; // 空间不足（槽位数组和数据区相遇）
                }
            }
            // 0 1 | 2 3 4 5 | 6 7 8 9

            // 分配槽位
            int slotIndex;
            if (!needNewSlot) {
                // 重用空闲槽位
                slotIndex = header.firstFreeSlot;
                Slot slot = slots.get(slotIndex);
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
            header.recordCount++;
            header.freeSpace -= requiredSpace;
            header.isDirty = true;

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
         * 原本数据还保留，但是槽位信息得改，并且相当于新增记录
         * 但是记录的长度可能不一样，要考虑空间够不够的问题
         */
        public boolean updateRecord(int slotIndex) {
            return false;
        }

        public PageHeader getHeader() {
            return header;
        }
    }

    // 随便写的
    public static void writePage(DataPage page, int spaceId) throws IOException {
    }

    public static DataPage readPage(GlobalPageId pageId) throws IOException {
        DataPage page = new DataPage(pageId.pageNo);
        return page;
    }
}