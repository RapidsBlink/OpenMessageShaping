package play;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by cheyulin on 27/05/2017.
 */
public class ByteBufferPlayGround {
    static void testPosition() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[1024]);
        System.out.println("pos:" + byteBuffer.position());

        byteBuffer.putInt(1);
        System.out.println("pos:" + byteBuffer.position());
        byteBuffer.putInt(2);
        System.out.println("pos:" + byteBuffer.position());
//        System.out.println(byteBuffer.getInt(0));
//        System.out.println(byteBuffer.getInt(0));
//        System.out.println(byteBuffer.getInt(4));
        byteBuffer.position(0);
        System.out.println(byteBuffer.getInt());
        System.out.println("pos:" + byteBuffer.position());

        System.out.println(byteBuffer.getInt());
        System.out.println(byteBuffer.getInt());
        System.out.println("pos:" + byteBuffer.position());
    }

    static void testPosition2() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[1024]);
        byteBuffer.order(ByteOrder.nativeOrder());
        System.out.println("pos:" + byteBuffer.position());

        byteBuffer.putInt(1);
        System.out.println("pos:" + byteBuffer.position());
        byteBuffer.putInt(2);
        byteBuffer.flip();

        System.out.println("limit:" + byteBuffer.limit());
        byte[] myBuf = new byte[byteBuffer.limit()];
        byteBuffer.get(myBuf);

        ByteBuffer anotherByteBuffer=ByteBuffer.wrap(new byte[1024]);
        anotherByteBuffer.order(ByteOrder.nativeOrder());
        anotherByteBuffer.put(myBuf);
        anotherByteBuffer.flip();
        //        System.out.println();
        for (int i = 0; i < 2; i++) {
            System.out.println(anotherByteBuffer.getInt());
            System.out.println("pos:" + anotherByteBuffer.position());
        }

    }

    static void testPosition3() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[1024]);
        byteBuffer.order(ByteOrder.nativeOrder());
        System.out.println("pos:" + byteBuffer.position());

        byteBuffer.putInt(1);
        System.out.println("pos:" + byteBuffer.position());
        byteBuffer.putInt(2);
        byteBuffer.flip();

        System.out.println("limit:" + byteBuffer.limit());
        byte[] myBuf = new byte[4];
        byteBuffer.get(myBuf);

        ByteBuffer anotherByteBuffer=ByteBuffer.wrap(new byte[1024]);
        anotherByteBuffer.order(ByteOrder.nativeOrder());
        anotherByteBuffer.put(myBuf);
        anotherByteBuffer.flip();
        //        System.out.println();
        for (int i = 0; i < 2; i++) {
            System.out.println(anotherByteBuffer.getInt());
            System.out.println("pos:" + anotherByteBuffer.position());
        }

    }

    public static void main(String[] args) {
        testPosition2();
    }
}
