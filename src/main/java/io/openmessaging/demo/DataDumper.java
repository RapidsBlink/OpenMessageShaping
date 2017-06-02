package io.openmessaging.demo;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by will on 31/5/2017.
 */
public class DataDumper {
    private static int CHUNK_SIZE = 128 * 1024 * 1024;

    private static boolean isInit = false;
    private static ReentrantLock initLock = new ReentrantLock();
    private static RandomAccessFile dataFile;
    private static FileChannel dataFileChannel;

    private static ObjectOutputStream outputStream;

    // for writing
    private static MappedByteBuffer mappedByteBuffer = null;
    private static ReentrantLock getOffsetLock = new ReentrantLock();
    private static int nextChunkIdx = 0;
    private static int nextInChunkOffset = 0;

    // for unmap
    private static AtomicInteger mapAreaUserNum = new AtomicInteger(0);

    // for close
    private static AtomicInteger numberOfProducer = new AtomicInteger(0);
    private static ReentrantLock closeLock = new ReentrantLock();
    private static int finishedNum = 0;

    private static DataIndexer dataIndexer = new DataIndexer();

    public DataDumper(String fileRootPath) throws IOException {
        initLock.lock();
        if (!isInit) {
            dataFile = new RandomAccessFile(fileRootPath + File.separator + "data.bin", "rw");
            long maxDataFileSize = 10L * 1024 * 1024 * 1024;
            dataFile.setLength(maxDataFileSize);
            dataFileChannel = dataFile.getChannel();
            outputStream = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fileRootPath + File.separator + "index.bin")));
            isInit = true;
        }
        initLock.unlock();

        numberOfProducer.incrementAndGet();
    }

    public void writeToFile(ByteBuffer data) throws IOException {
        int offset = getInChunkOffset(data.limit() + Integer.BYTES);

        // 1st: length of byte arr
        mappedByteBuffer.putInt(offset, data.limit());
        offset += Integer.BYTES;
        // 2nd: byte arr
        for (int i = 0; i < data.limit(); i++) {
            mappedByteBuffer.put(offset + i, data.get(i));
        }

        // record worker num in current mapped area
        mapAreaUserNum.decrementAndGet();
    }

    private void getNextChunk() throws IOException {
        // need to unmap
        if (mappedByteBuffer != null) {
            // need to make sure no other threads use this mapped area, busy waiting here
            while (mapAreaUserNum.get() >= 0) {
                if (mapAreaUserNum.get() == 0) {
                    unmap(mappedByteBuffer);
                    break;
                }
            }
        }
        mappedByteBuffer = dataFileChannel.map(FileChannel.MapMode.PRIVATE, (long) nextChunkIdx * CHUNK_SIZE, CHUNK_SIZE);
        nextChunkIdx += 1;
    }

    private int getInChunkOffset(int dataLength) throws IOException {
        getOffsetLock.lock();

        int nextInChunkOffset = DataDumper.nextInChunkOffset + dataLength;
        int wasteSize = 256 * 1024;
        if (CHUNK_SIZE - nextInChunkOffset < wasteSize || mappedByteBuffer == null) {
            if (mappedByteBuffer != null) {
                dataIndexer.mmapChunkLengthList.add(nextInChunkOffset);
            }
            getNextChunk();
            nextInChunkOffset = 0;
        }
        DataDumper.nextInChunkOffset = nextInChunkOffset;

        mapAreaUserNum.incrementAndGet();

        getOffsetLock.unlock();

        return nextInChunkOffset;
    }

    private static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
        } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    void close() throws IOException {
        closeLock.lock();
        finishedNum += 1;
        System.out.println("close from thread " + Thread.currentThread().getName() + ", total num:" + numberOfProducer.get());
        if (finishedNum == numberOfProducer.get()) {
            // truncate file
            System.out.println("curChunkIdx:" + (nextChunkIdx - 1));
            System.out.println("nextInChunkOffset:" + nextInChunkOffset);
            long fileSize = (long) (nextChunkIdx - 1) * CHUNK_SIZE + nextInChunkOffset;
            System.out.println("fileLen:" + fileSize);
            dataFile.setLength(fileSize);
            dataFile.close();

            // index info for recording chunk length
            if (nextInChunkOffset != 0) {
                dataIndexer.mmapChunkLengthList.add(nextInChunkOffset);
            }
            dataIndexer.mmapChunkLengthList.forEach(System.out::println);
            outputStream.writeObject(dataIndexer);
            outputStream.close();

        }
        closeLock.unlock();
    }
}

