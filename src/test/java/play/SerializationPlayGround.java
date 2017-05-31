package play;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by cheyulin on 24/05/2017.
 */

import io.openmessaging.demo.DefaultKeyValue;
import javafx.util.Pair;

public class SerializationPlayGround {
    private static final Logger logger = Logger.getLogger("App");

    private static byte[] toByteArray(double value) {
        byte[] bytes = new byte[Double.BYTES];
        ByteBuffer.wrap(bytes).putDouble(value);
        return bytes;
    }

    private static double toDouble(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getDouble();
    }

    private static byte[] longToBytes(long x) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private static long bytesToLong(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static int efficientByteArrayToLeInt(byte[] b) {
        final ByteBuffer bb = ByteBuffer.wrap(b);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        return bb.getInt();
    }

    public static byte[] efficientLeIntToByteArray(int i) {
        final ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putInt(i);
        return bb.array();
    }

    public static int byteArrayToLeInt(byte[] encodedValue) {
        int value = (encodedValue[3] << (Byte.SIZE * 3));
        value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
        value |= (encodedValue[1] & 0xFF) << (Byte.SIZE);
        value |= (encodedValue[0] & 0xFF);
        return value;
    }

    public static byte[] leIntToByteArray(int value) {
        byte[] encodedValue = new byte[Integer.SIZE / Byte.SIZE];
        encodedValue[3] = (byte) (value >> Byte.SIZE * 3);
        encodedValue[2] = (byte) (value >> Byte.SIZE * 2);
        encodedValue[1] = (byte) (value >> Byte.SIZE);
        encodedValue[0] = (byte) value;
        return encodedValue;
    }

    private static void playLongDoubleStr() {
        String myString = "TOPIC_A";
        System.out.print("len:" + myString.length() + ",");
        for (byte myByte : myString.getBytes()) {
            System.out.print(myByte);
            System.out.print(',');
        }
        System.out.println();
        System.out.println(new String(myString.getBytes()) + "\n");

        long myLong = 1L;
        System.out.println("long byte len:" + longToBytes(myLong).length);
        for (byte myByte : longToBytes(myLong)) {
            System.out.print(myByte);
        }
        System.out.println();
        System.out.println(bytesToLong(longToBytes(myLong)) + "\n");

        double myDouble = 1.0D;
        System.out.println("double byte len:" + toByteArray(myDouble).length);
        for (byte myByte : toByteArray(myDouble)) {
            System.out.print(myByte);
        }
        System.out.println();
        System.out.println(toDouble(toByteArray(myDouble)) + "\n");

        int myInt = 2;
        System.out.println(efficientByteArrayToLeInt(efficientLeIntToByteArray(myInt)) + "\n");
    }


    private static void playReorderedMap() {
        DefaultKeyValue keyValue = new DefaultKeyValue();
        keyValue.put("key1", 1);
        keyValue.put("key2", (short) 2);
        keyValue.put("key3", 3L);
        keyValue.put("key4", 4D);
        keyValue.put("key5", "str");
        List<Pair<String, Integer>> integerList = new ArrayList<>();
        List<Pair<String, Long>> longList = new ArrayList<>();
        List<Pair<String, Double>> doubleList = new ArrayList<>();
        List<Pair<String, String>> stringList = new ArrayList<>();

        keyValue.kvs.forEach((k, v) -> {
            if (v instanceof Integer) {
                integerList.add(new Pair<>(k, (Integer) v));
            } else if (v instanceof Long) {
                longList.add(new Pair<>(k, (Long) v));
            } else if (v instanceof Double) {
                doubleList.add(new Pair<>(k, (Double) v));
            } else {
                stringList.add(new Pair<>(k, (String) v));
            }
        });
        System.out.println(integerList);
        System.out.println(longList);
        System.out.println(doubleList);
        System.out.println(stringList);
    }

    private static void playLogger() {
        logger.info("over");
        logger.log(Level.SEVERE, "fsd");
    }

    public static void main(String[] args) {
        playLongDoubleStr();
        playReorderedMap();
    }
}
