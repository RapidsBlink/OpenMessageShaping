package io.openmessaging.demo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by yche on 6/3/17.
 */
public class Util {
    public static int byteArrayToLeInt(byte[] encodedValue) {
        int value = (encodedValue[3] << (Byte.SIZE * 3));
        value |= (encodedValue[2] & 0xFF) << (Byte.SIZE * 2);
        value |= (encodedValue[1] & 0xFF) << (Byte.SIZE);
        value |= (encodedValue[0] & 0xFF);
        return value;
    }


    public final void writeInt(OutputStream outputStream, int v) throws IOException {
        outputStream.write((v >>> 24) & 0xFF);
        outputStream.write((v >>> 16) & 0xFF);
        outputStream.write((v >>> 8) & 0xFF);
        outputStream.write((v >>> 0) & 0xFF);
    }

}
