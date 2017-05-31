package io.openmessaging.demo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * can only be used by a single object
 * otherwise incurs failures
 * Created by will on 27/5/2017.
 */
public class MessageSerialization {
    private static final int MAX_MESSAGE_SIZE = 1024 * 512;
    private final ByteBuffer keyValueByteBuffer;
    private final ByteBuffer messageByteBuffer;

    private final List<YchePair<String, Integer>> integerList = new ArrayList<>();
    private final List<YchePair<String, Long>> longList = new ArrayList<>();
    private final List<YchePair<String, Double>> doubleList = new ArrayList<>();
    private final List<YchePair<String, String>> stringList = new ArrayList<>();

    public MessageSerialization() {
        keyValueByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        messageByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
    }

    private void putLenAndBytesToByteBuffer(byte[] bytes, ByteBuffer byteBuffer) {
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
    }

    private void putStringToByteBuffer(String myString, ByteBuffer byteBuffer) {
        putLenAndBytesToByteBuffer(myString.getBytes(), byteBuffer);
    }

    private void serializeHashMapIntoKeyValueBuffer(DefaultKeyValue kv) {
        // write sequentially to buffer
        keyValueByteBuffer.clear();

        integerList.clear();
        longList.clear();
        doubleList.clear();
        stringList.clear();

        kv.kvs.forEach((keyString, valObject) -> {
            if (valObject instanceof Integer) {
                integerList.add(new YchePair<>(keyString, (Integer) valObject));
            } else if (valObject instanceof Long) {
                longList.add(new YchePair<>(keyString, (Long) valObject));
            } else if (valObject instanceof Double) {
                doubleList.add(new YchePair<>(keyString, (Double) valObject));
            } else {
                stringList.add(new YchePair<>(keyString, (String) valObject));
            }
        });

        // 1st: length
        keyValueByteBuffer.putInt(integerList.size());
        keyValueByteBuffer.putInt(longList.size());
        keyValueByteBuffer.putInt(doubleList.size());
        keyValueByteBuffer.putInt(stringList.size());

        // 2nd: pair info
        integerList.forEach((strIntegerPair) -> {
            putStringToByteBuffer(strIntegerPair.key, keyValueByteBuffer);
            keyValueByteBuffer.putInt(strIntegerPair.val);
        });

        longList.forEach((strLongPair) -> {
            putStringToByteBuffer(strLongPair.key, keyValueByteBuffer);
            keyValueByteBuffer.putLong(strLongPair.val);
        });

        doubleList.forEach((strDoublePair) -> {
            putStringToByteBuffer(strDoublePair.key, keyValueByteBuffer);
            keyValueByteBuffer.putDouble(strDoublePair.val);
        });

        stringList.forEach((strStringPair) -> {
            putStringToByteBuffer(strStringPair.key, keyValueByteBuffer);
            putStringToByteBuffer(strStringPair.val, keyValueByteBuffer);
        });

        // read from buffer, get byte[]
        keyValueByteBuffer.flip();
    }

    private void serializeDetail(DefaultBytesMessage message) {
        // write to buffer sequentially
        messageByteBuffer.clear();

        // 1st: write body
        messageByteBuffer.putInt(message.getBody().length);
        messageByteBuffer.put(message.getBody());

        // 2nd: write headers, properties (i.e., key_value info)
        serializeHashMapIntoKeyValueBuffer((DefaultKeyValue) message.headers());
        messageByteBuffer.putInt(keyValueByteBuffer.limit());
        messageByteBuffer.put(keyValueByteBuffer);

        serializeHashMapIntoKeyValueBuffer((DefaultKeyValue) message.properties());
        messageByteBuffer.putInt(keyValueByteBuffer.limit());
        messageByteBuffer.put(keyValueByteBuffer);

        // 3rd: make buffer readable
        messageByteBuffer.flip();
    }

    public void serialize(DefaultBytesMessage message, ByteBuffer byteBuffer) {
        serializeDetail(message);
        byteBuffer.put(messageByteBuffer);
    }

    public int serialize(DefaultBytesMessage message, byte[] byteArr, int offset) {
        serializeDetail(message);
        for (int i = 0; i < messageByteBuffer.limit(); i++) {
            byteArr[offset + i] = messageByteBuffer.get();
        }
        return messageByteBuffer.limit();
    }

    public byte[] serialize(DefaultBytesMessage message) {
        serializeDetail(message);
        byte[] retBytes = new byte[messageByteBuffer.limit()];
        messageByteBuffer.get(retBytes);
        return retBytes;
    }
}
