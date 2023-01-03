package com.paypal.dione.avro.hadoop.file;

import java.util.LinkedHashMap;
import java.util.Map;

public class AvroBtreeFileUtils {

    private AvroBtreeFileUtils() {}

    public static class LRUCache extends LinkedHashMap {
        private int size;

        public LRUCache(int size) {
            super(size, 0.75f, true);
            this.size = size;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > size;
        }
    }
}
