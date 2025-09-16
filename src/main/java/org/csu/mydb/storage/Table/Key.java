package org.csu.mydb.storage.Table;

import org.csu.mydb.storage.Table.Column.Column;

import java.util.List;

public class Key implements Comparable<Key> {
    private final List<Object> values;
    private final List<Column> keyColumns;

    public Key(List<Object> values, List<Column> keyColumns) {
        this.values = values;
        this.keyColumns = keyColumns;
    }

    @Override
    public int compareTo(Key other) {
        for (int i = 0; i < keyColumns.size(); i++) {
            Column col = keyColumns.get(i);
            Comparable v1 = (Comparable) values.get(i);
            Comparable v2 = (Comparable) other.values.get(i);

            if (v1 == null && v2 == null) continue;
            if (v1 == null) return -1;
            if (v2 == null) return 1;

            int cmp = v1.compareTo(v2);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @Override
    public String toString() {
        return values.toString();
    }

    public List<Object> getValues() {
        return values;
    }

    public List<Column> getKeyColumns() {
        return keyColumns;
    }
}

