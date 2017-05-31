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
    private final ByteBuffer serializationMessageByteBuffer;

    private final List<YchePair<String, Integer>> integerList = new ArrayList<>();
    private final List<YchePair<String, Long>> longList = new ArrayList<>();
    private final List<YchePair<String, Double>> doubleList = new ArrayList<>();
    private final List<YchePair<String, String>> stringList = new ArrayList<>();

    public MessageSerialization() {
        keyValueByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        serializationMessageByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
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

    public ByteBuffer serialize(DefaultBytesMessage message, ByteBuffer messageByteBuffer) {
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
        return messageByteBuffer;
    }

    public int serialize(DefaultBytesMessage message, byte[] byteArr, int offset) {
        serialize(message, serializationMessageByteBuffer);
        for (int i = 0; i < serializationMessageByteBuffer.limit(); i++) {
            byteArr[offset + i] = serializationMessageByteBuffer.get();
        }
        return serializationMessageByteBuffer.limit();
    }

    public byte[] serializeIntoNewByteArr(DefaultBytesMessage message) {
        serialize(message, serializationMessageByteBuffer);
        byte[] retBytes = new byte[serializationMessageByteBuffer.limit()];
        serializationMessageByteBuffer.get(retBytes);
        return retBytes;
    }
}
