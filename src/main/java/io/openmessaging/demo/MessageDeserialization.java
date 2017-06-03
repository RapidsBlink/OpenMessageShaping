package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by yche on 5/27/17.
 */
public class MessageDeserialization {
    private static final int MAX_MESSAGE_SIZE = 1024 * 260;

    private final ByteBuffer messageByteBuffer;

    public MessageDeserialization() {
        messageByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
    }

    private String getStringFromByteBuffer() {
        int lenBytes = messageByteBuffer.getInt();
        byte[] bytes = new byte[lenBytes];
        messageByteBuffer.get(bytes);
        return new String(bytes);
    }

    private DefaultKeyValue deSerializeHashMap() {
        DefaultKeyValue defaultKeyValue = new DefaultKeyValue();

        // 1st: get corresponding pair count
        int integerListLen = messageByteBuffer.getShort();
        int longListLen = messageByteBuffer.getShort();
        int doubleListLen = messageByteBuffer.getShort();
        int strListLen = messageByteBuffer.getShort();

        // 2nd: loop to get pairs
        String key;
        for (int i = 0; i < integerListLen; i++) {

            key = getStringFromByteBuffer();
            int intVal = messageByteBuffer.getInt();
            defaultKeyValue.put(key, intVal);
        }
        for (int i = 0; i < longListLen; i++) {
            key = getStringFromByteBuffer();
            long longVal = messageByteBuffer.getLong();
            defaultKeyValue.put(key, longVal);
        }
        for (int i = 0; i < doubleListLen; i++) {
            key = getStringFromByteBuffer();
            double doubleVal = messageByteBuffer.getDouble();
            defaultKeyValue.put(key, doubleVal);
        }
        for (int i = 0; i < strListLen; i++) {
            key = getStringFromByteBuffer();
            String strVal = getStringFromByteBuffer();
            defaultKeyValue.put(key, strVal);
        }

        return defaultKeyValue;
    }

    public DefaultBytesMessage deserialize(byte[] byteBuffer, int offset, int length) {
        messageByteBuffer.clear();
        messageByteBuffer.put(byteBuffer, offset, length);

        messageByteBuffer.flip();
        int lenBytes = messageByteBuffer.getInt();
        byte[] body = new byte[lenBytes];
        messageByteBuffer.get(body);

        KeyValue headerKeyValue = deSerializeHashMap();
        KeyValue propertyKeyValue = deSerializeHashMap();

        return new DefaultBytesMessage(body, headerKeyValue, propertyKeyValue);
    }

    public DefaultBytesMessage deserialize(byte[] byteMessage) {
        return deserialize(byteMessage, 0, byteMessage.length);
    }

    public DefaultBytesMessage deserialize(ByteBuffer byteMessage) {
        messageByteBuffer.clear();
        messageByteBuffer.put(byteMessage);

        messageByteBuffer.flip();
        int lenBytes = messageByteBuffer.getInt();
        byte[] body = new byte[lenBytes];
        messageByteBuffer.get(body);

        KeyValue headerKeyValue = deSerializeHashMap();
        KeyValue propertyKeyValue = deSerializeHashMap();

        return new DefaultBytesMessage(body, headerKeyValue, propertyKeyValue);
    }
}
