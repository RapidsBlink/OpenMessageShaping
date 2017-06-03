package io.openmessaging.demo.unitTest;

import io.openmessaging.MessageHeader;
import io.openmessaging.demo.CompressionUtils;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.MessageSerialization;
import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.DataFormatException;

/**
 * Created by yche on 6/3/17.
 */
public class CompressionTester {
    private static ArrayList<String> topics = new ArrayList<>();
    private static MessageSerialization messageSerialization = new MessageSerialization();
    private static ByteBuffer messageBinary = ByteBuffer.allocate(260 * 1024);

    private static ByteBuffer globalBuffer = ByteBuffer.allocate(60 * 1024 * 1024);

    private static void initTopics() {
        for (int i = 0; i < 70; i++) {
            topics.add("TOPIC_" + i);
        }
    }

    private static void initSerializationData() {
        for (int i = 0; i < 10000; i++) {
            Random rnd = new Random();
            String topic = topics.get(rnd.nextInt(70));
            int length = 80;
            if (i % 100000 == 0) {
                length = 250 * 1024;
            }

            byte[] input = new byte[length];
            for (int j = 0; j < length; j++) {
                input[j] = (byte) rnd.nextInt(128);
            }

            DefaultBytesMessage message = new DefaultBytesMessage(input);
            message.putHeaders(MessageHeader.TOPIC, topic);
            message.putProperties("id", i % 128);

            messageBinary.clear();
            messageSerialization.serialize(message, messageBinary);
            messageBinary.flip();

            globalBuffer.put(messageBinary);
        }
    }

    public static void main(String[] args) throws IOException, DataFormatException {
        initTopics();
        initSerializationData();
        globalBuffer.flip();
        System.out.println(globalBuffer.limit());
        byte[] compressBytes = CompressionUtils.compress(globalBuffer.array(), 0, globalBuffer.limit());
        System.out.println(compressBytes.length);
        byte[] decompressedBytes = CompressionUtils.decompress(compressBytes, 0, compressBytes.length);
        System.out.println(decompressedBytes.length);
        byte[] anotherCompressedBytes = CompressionUtils.compress(decompressedBytes, 0, decompressedBytes.length);
        System.out.println(anotherCompressedBytes.length);
        System.out.println(Arrays.equals(compressBytes, anotherCompressedBytes));
    }
}
