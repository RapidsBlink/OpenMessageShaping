package io.openmessaging.demo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Created by yche on 6/3/17.
 */
public class CompressionUtils {
    //private static final Logger LOG = Logger.getLogger(String.valueOf(CompressionUtils.class));

    public static byte[] compress(byte[] data, int offset, int length) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(data, offset, length);
        //deflater.setLevel(Deflater.BEST_COMPRESSION);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

        deflater.finish();
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer); // returns the generated code... index
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        deflater.end();

        //LOG.info("Original: " + data.length / 1024 + " Kb");
        //LOG.info("Compressed: " + output.length / 1024 + " Kb");
        return output;
    }

    public static byte[] decompress(byte[] data, int offset, int length) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();

        inflater.setInput(data, offset,length);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        byte[] output = outputStream.toByteArray();

        inflater.end();

        //LOG.info("Original: " + data.length);
        //LOG.info("Uncompressed: " + output.length);
        return output;
    }
}

