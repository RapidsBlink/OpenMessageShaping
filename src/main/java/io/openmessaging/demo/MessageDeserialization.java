package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by yche on 5/27/17.
 */
public class MessageDeserialization {
    private static final int MAX_MESSAGE_SIZE = 1024 * 512;

    private final ByteBuffer keyValueByteBuffer;
    private final ByteBuffer messageByteBuffer;

    public MessageDeserialization() {
        keyValueByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
        messageByteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
    }

    private byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
        int bytesLen = byteBuffer.getInt();

        byte[] myBytes = new byte[bytesLen];
        for (int i = 0; i < bytesLen; i++) {
            myBytes[i] = byteBuffer.get();
        }
        return myBytes;
    }

    private String getStringFromByteBuffer(ByteBuffer byteBuffer) {
        return new String(getBytesFromByteBuffer(byteBuffer));
    }

    private DefaultKeyValue deSerializeHashMap(byte[] keyValueBytes) {
        DefaultKeyValue defaultKeyValue = new DefaultKeyValue();
        keyValueByteBuffer.clear();
        keyValueByteBuffer.put(keyValueBytes);

        // read
        keyValueByteBuffer.flip();

        // 1st: get corresponding pair count
        int integerListLen = keyValueByteBuffer.getInt();
        int longListLen = keyValueByteBuffer.getInt();
        int doubleListLen = keyValueByteBuffer.getInt();
        int strListLen = keyValueByteBuffer.getInt();

        // 2nd: loop to get pairs
        String key;
        for (int i = 0; i < integerListLen; i++) {
            key = getStringFromByteBuffer(keyValueByteBuffer);
            int intVal = keyValueByteBuffer.getInt();
            defaultKeyValue.put(key, intVal);
        }
        for (int i = 0; i < longListLen; i++) {
            key = getStringFromByteBuffer(keyValueByteBuffer);
            long longVal = keyValueByteBuffer.getLong();
            defaultKeyValue.put(key, longVal);
        }
        for (int i = 0; i < doubleListLen; i++) {
            key = getStringFromByteBuffer(keyValueByteBuffer);
            double doubleVal = keyValueByteBuffer.getDouble();
            defaultKeyValue.put(key, doubleVal);
        }
        for (int i = 0; i < strListLen; i++) {
            key = getStringFromByteBuffer(keyValueByteBuffer);
            String strVal = getStringFromByteBuffer(keyValueByteBuffer);
            defaultKeyValue.put(key, strVal);
        }

        return defaultKeyValue;
    }

    public DefaultBytesMessage deserialize(byte[] byteBuffer, int offset, int length) {
        messageByteBuffer.clear();
        messageByteBuffer.put(byteBuffer, offset, length);

        messageByteBuffer.flip();
        byte[] body = getBytesFromByteBuffer(messageByteBuffer);
        byte[] headers = getBytesFromByteBuffer(messageByteBuffer);
        byte[] properties = getBytesFromByteBuffer(messageByteBuffer);

        KeyValue headerKeyValue = deSerializeHashMap(headers);
        KeyValue propertyKeyValue = deSerializeHashMap(properties);

        return new DefaultBytesMessage(body, headerKeyValue, propertyKeyValue);
    }

    public DefaultBytesMessage deserialize(byte[] byteMessage) {
        return deserialize(byteMessage, 0, byteMessage.length);
    }

//    // byteMessage only contains one byte[], and is ready for reading
//    // i.e, already flip()
    public DefaultBytesMessage deserialize(ByteBuffer byteMessage) {
        messageByteBuffer.clear();
        messageByteBuffer.put(byteMessage);

        messageByteBuffer.flip();
        byte[] body = getBytesFromByteBuffer(messageByteBuffer);
        byte[] headers = getBytesFromByteBuffer(messageByteBuffer);
        byte[] properties = getBytesFromByteBuffer(messageByteBuffer);

        KeyValue headerKeyValue = deSerializeHashMap(headers);
        KeyValue propertyKeyValue = deSerializeHashMap(properties);

        return new DefaultBytesMessage(body, headerKeyValue, propertyKeyValue);
    }
}
