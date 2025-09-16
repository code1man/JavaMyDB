package org.csu.mydb.storage.storageFiles.page;

import org.csu.mydb.storage.PageManager;
import org.csu.mydb.storage.Table.Column.Column;
import org.csu.mydb.storage.Table.Key;
import org.csu.mydb.storage.storageFiles.page.record.RecordHead;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PageSorter {

    /**
     * 记录条目（用于排序）
     */
    private static class RecordEntry {
        int slotIndex;              // 槽位索引
        PageManager.Slot slot;      // 槽位对象
        RecordHead head;            // 记录头
        Key key;                    // 主键值
        byte[] recordData;          // 完整记录数据

        RecordEntry(int slotIndex, PageManager.Slot slot, RecordHead head, Key key, byte[] recordData) {
            this.slotIndex = slotIndex;
            this.slot = slot;
            this.head = head;
            this.key = key;
            this.recordData = recordData;
        }
    }

    /**
     * 对页内记录按主键排序
     * @param page 要排序的页
     * @param allColumns 所有列定义（用于数据页）
     * @param primaryKeyColumns 主键列定义（用于索引页）
     */
    public static void sortPageByPrimaryKey(PageManager.Page page,
                                            List<Column> allColumns,
                                            List<Column> primaryKeyColumns) {
        // 1. 提取所有有效记录及其主键值
        List<RecordEntry> entries = extractRecordEntries(page, allColumns, primaryKeyColumns);

        // 2. 按主键排序
        entries.sort(Comparator.comparing(e -> e.key));

        // 3. 更新槽位顺序
        updateSlotOrder(page, entries);

        // 4. 重新链接记录链表
        relinkRecords(page, entries);

    }

    /**
     * 提取记录信息
     */
    private static List<RecordEntry> extractRecordEntries(PageManager.Page page,
                                                          List<Column> allColumns,
                                                          List<Column> primaryKeyColumns) {
        List<RecordEntry> entries = new ArrayList<>();

        for (int slotIndex = 0; slotIndex < page.getSlots().size(); slotIndex++) {
            PageManager.Slot slot = page.getSlots().get(slotIndex);
            if (slot.status != 1) continue; // 只处理有效记录

            byte[] recordData = page.getRecord(slotIndex);
            if (recordData == null) continue;

            // 解析记录头
            RecordHead head = parseRecordHead(recordData);

            // 提取主键值（根据页类型选择不同方法）
            Key key = KeyExtractor.extractPrimaryKey(
                    recordData,
                    page.header.pageType,
                    allColumns,
                    primaryKeyColumns
            );

            entries.add(new RecordEntry(slotIndex, slot, head, key, recordData));
        }

        return entries;
    }

    /**
     * 解析记录头
     */
    private static RecordHead parseRecordHead(byte[] recordData) {
        ByteBuffer buffer = ByteBuffer.wrap(recordData);
        return new RecordHead(
                buffer.get(), // isDeleted
                buffer.get(), // recordType
                buffer.getShort()   // nextRecordOffset
        );
    }

    /**
     * 更新槽位顺序
     */
    private static void updateSlotOrder(PageManager.Page page, List<RecordEntry> sortedEntries) {
        // 创建新的槽位列表（保持原始物理位置）
        List<PageManager.Slot> newSlots = new ArrayList<>(page.getSlots());

        // 更新槽位指向的记录索引
        for (int i = 0; i < sortedEntries.size(); i++) {
            RecordEntry entry = sortedEntries.get(i);
            newSlots.set(i, entry.slot);
        }

        page.slots = newSlots;
        page.header.slotCount = (short) newSlots.size();
    }

    /**
     * 重新链接记录链表
     */
    private static void relinkRecords(PageManager.Page page, List<RecordEntry> sortedEntries) {
        for (int i = 0; i < sortedEntries.size(); i++) {
            RecordEntry current = sortedEntries.get(i);
            RecordEntry next = (i < sortedEntries.size() - 1) ? sortedEntries.get(i + 1) : null;

            // 计算新的下一条记录偏移量
            if (next != null) {
                int newOffset = next.slot.offset - current.slot.offset;
                current.head.setNextRecord((short) newOffset);
            } else {
                current.head.setNextRecord((short) 0); // 最后一条记录
            }

            // 更新记录数据
            updateRecordData(page, current);
        }
    }

    /**
     * 更新记录数据（主要是更新记录头）
     */
    private static void updateRecordData(PageManager.Page page, RecordEntry entry) {
        ByteBuffer buffer = ByteBuffer.wrap(entry.recordData);

        // 更新记录头
        buffer.put(entry.head.isDeleted());
        buffer.put(entry.head.isRecordType());
        buffer.putShort(entry.head.getNextRecord());

        // 写回页数据
        System.arraycopy(
                buffer.array(), 0,
                page.pageData, entry.slot.offset,
                entry.recordData.length
        );
    }
}