package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DefaultKeyValue implements KeyValue {
    final Map<String, String> kvs = new HashMap<>();

    @Override
    public KeyValue put(String key, int value) {
        kvs.put(key, Integer.toString(value));
        return this;
    }

    @Override
    public KeyValue put(String key, long value) {
        kvs.put(key, Long.toString(value));
        return this;
    }

    @Override
    public KeyValue put(String key, double value) {
        kvs.put(key, Double.toString(value));
        return this;
    }

    @Override
    public KeyValue put(String key, String value) {
        kvs.put(key, value);
        return this;
    }

    @Override
    public int getInt(String key) {
        return Integer.valueOf(kvs.getOrDefault(key, "0"));
    }

    @Override
    public long getLong(String key) {
        return Long.valueOf(kvs.getOrDefault(key, "0"));
    }

    @Override
    public double getDouble(String key) {
        return Double.valueOf(kvs.getOrDefault(key, "0.0"));
    }

    @Override
    public String getString(String key) {
        return kvs.getOrDefault(key, null);
    }

    @Override
    public Set<String> keySet() {
        return kvs.keySet();
    }

    @Override
    public boolean containsKey(String key) {
        return kvs.containsKey(key);
    }
}