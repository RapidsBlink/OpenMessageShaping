package io.openmessaging.demo.unitTest;

import io.openmessaging.demo.DataIndexer;
import io.openmessaging.demo.MessageDeserialization;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created by yche on 6/2/17.
 */
public class FileReader {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String fileRootPath = "/tmp/test";
        RandomAccessFile dataFile = new RandomAccessFile(fileRootPath + File.separator + "data.bin", "rw");
        FileChannel dataFileChannel = dataFile.getChannel();
        ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fileRootPath + File.separator + "index.bin")));
        DataIndexer dataIndexer = ((DataIndexer) ois.readObject());
        ArrayList<Integer> mmapChunkLengthList = dataIndexer.mmapChunkLengthList;
        ArrayList<Integer> dataLen = dataIndexer.messageBytesLengthList;

        int CHUNK_SIZE = 128 * 1024 * 1024;
        ByteBuffer messageBinary = ByteBuffer.allocate(260 * 1024);
        MessageDeserialization messageDeserialization = new MessageDeserialization();

        for (int i = 1; i < mmapChunkLengthList.size(); i++) {
            MappedByteBuffer mappedByteBuffer = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, CHUNK_SIZE * i, mmapChunkLengthList.get(i));
            mappedByteBuffer.load();
            while (mappedByteBuffer.hasRemaining()) {
                int dataLength = mappedByteBuffer.getInt();
                System.out.println(dataLength);
                if (dataLength == 0) {
                    System.out.println("fuck");
                }
                messageBinary.clear();
                for (int idx = 0; idx < dataLength; idx++) {
                    messageBinary.put(mappedByteBuffer.get());
                }
                messageBinary.flip();
                System.out.println(new String(messageDeserialization.deserialize(messageBinary).getBody()));
            }
        }
    }

}
