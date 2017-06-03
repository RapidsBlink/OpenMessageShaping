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
    private static final int MAX_MESSAGE_SIZE = 1024 * 260;
    private final ByteBuffer messageByteBuffer;

    private final List<YchePair<String, Integer>> integerList = new ArrayList<>();
    private final List<YchePair<String, Long>> longList = new ArrayList<>();
    private final List<YchePair<String, Double>> doubleList = new ArrayList<>();
    private final List<YchePair<String, String>> stringList = new ArrayList<>();

    public MessageSerialization() {
        messageByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
    }

    private void putStringToByteBuffer(String myString, ByteBuffer byteBuffer) {
        byte[] myBytes = myString.getBytes();
        byteBuffer.putInt(myBytes.length);
        byteBuffer.put(myBytes);
    }

    private void serializeHashMapIntoKeyValueBuffer(DefaultKeyValue kv) {
        // write sequentially to buffer
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
        messageByteBuffer.putShort((short) integerList.size());
        messageByteBuffer.putShort((short) longList.size());
        messageByteBuffer.putShort((short) doubleList.size());
        messageByteBuffer.putShort((short) stringList.size());

        // 2nd: pair info
        integerList.forEach((strIntegerPair) -> {
            putStringToByteBuffer(strIntegerPair.key, messageByteBuffer);
            messageByteBuffer.putInt(strIntegerPair.val);
        });

        longList.forEach((strLongPair) -> {
            putStringToByteBuffer(strLongPair.key, messageByteBuffer);
            messageByteBuffer.putLong(strLongPair.val);
        });

        doubleList.forEach((strDoublePair) -> {
            putStringToByteBuffer(strDoublePair.key, messageByteBuffer);
            messageByteBuffer.putDouble(strDoublePair.val);
        });

        stringList.forEach((strStringPair) -> {
            putStringToByteBuffer(strStringPair.key, messageByteBuffer);
            putStringToByteBuffer(strStringPair.val, messageByteBuffer);
        });
    }

    private void serializeDetail(DefaultBytesMessage message) {
        // write to buffer sequentially
        messageByteBuffer.clear();

        // 1st: write body
        messageByteBuffer.putInt(message.getBody().length);
        messageByteBuffer.put(message.getBody());

        // 2nd: write headers, properties (i.e., key_value info)
        serializeHashMapIntoKeyValueBuffer((DefaultKeyValue) message.headers());
        serializeHashMapIntoKeyValueBuffer((DefaultKeyValue) message.properties());

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
