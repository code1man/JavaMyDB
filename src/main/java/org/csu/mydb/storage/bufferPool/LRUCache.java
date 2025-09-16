package org.csu.mydb.storage.bufferPool;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache<K, V> {
    private final int capacity;
    private final LinkedHashMap<K, V> lruMap;
    private final Lock lock;

    public LRUCache(int capacity) {
        this.capacity = capacity;
        this.lock = new ReentrantLock();

        this.lruMap = new LinkedHashMap<K, V>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > capacity;
            }
        };
    }

    public void put(K key, V value) {
        lock.lock();
        try {
            lruMap.put(key, value);
        } finally {
            lock.unlock();
        }
    }

    public V get(K key) {
        lock.lock();
        try {
            return lruMap.get(key);
        } finally {
            lock.unlock();
        }
    }

    public void evict() {
        lock.lock();
        try {
            if (!lruMap.isEmpty()) {
                K eldestKey = getEldestKey();
                lruMap.remove(eldestKey);
            }
        } finally {
            lock.unlock();
        }
    }

    public int size() {
        lock.lock();
        try {
            return lruMap.size();
        } finally {
            lock.unlock();
        }
    }

    public K getEldestKey() {
        lock.lock();
        try {
            if (lruMap.isEmpty()) {
                return null;
            }
            return lruMap.keySet().iterator().next();
        } finally {
            lock.unlock();
        }
    }

    public void remove(K key) {
        lock.lock();
        try {
            lruMap.remove(key);
        } finally {
            lock.unlock();
        }
    }
}

