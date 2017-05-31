package io.openmessaging.demo;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by will on 31/5/2017.
 */
public class DataDumper {
    private static String rootPath;
    private static long DATA_FILE_SIZE = 8L * 1024 * 1024 * 1024; // 6GB

    private static RandomAccessFile dataFile;
    private static FileChannel dataFileChannel;
    private static DataFileIndexer dataFileIndexer = new DataFileIndexer();
    private static int MINI_CHUNK_SIZE = dataFileIndexer.MINI_CHUNK_SIZE;

    private static MappedByteBuffer[] topicMappedBuff = new MappedByteBuffer[dataFileIndexer.INIT_MAX_TOPIC_NUMBER];

    private static ReentrantLock[] topicWriteLocks = new ReentrantLock[dataFileIndexer.INIT_MAX_TOPIC_NUMBER];
    private static AtomicInteger numberOfProducer = new AtomicInteger(0);
    private static AtomicInteger numberOfFinished = new AtomicInteger(0);

    private static boolean isInit = false;
    private static ReentrantLock initLock = new ReentrantLock();

    public DataDumper(String fileRootPath) throws IOException {
        initLock.lock();
        if (!isInit) {
            rootPath = fileRootPath;
            dataFile = new RandomAccessFile(fileRootPath + File.separator + "data.bin", "rw");
            dataFile.setLength(DATA_FILE_SIZE);
            dataFileChannel = dataFile.getChannel();
            for (int i = 0; i < dataFileIndexer.INIT_MAX_TOPIC_NUMBER; i++) {
                topicMappedBuff[i] = null;
                topicWriteLocks[i] = new ReentrantLock();
            }
            isInit = true;
        }
        initLock.unlock();

        numberOfProducer.incrementAndGet();
    }

    public void writeToFile(String topicName, byte[] data) throws IOException {
        int topicNumber = dataFileIndexer.getAssignedTopicNumber(topicName);

        int offset = getMessageWriteOffset(topicNumber, data.length + Integer.BYTES);

        MappedByteBuffer buf = topicMappedBuff[topicNumber];

        // 1st: length of byte arr
        buf.putInt(offset, data.length);
        offset += Integer.BYTES;
        // 2nd: byte arr
        for (int i = 0; i < data.length; i++) {
            buf.put(offset + i, data[i]);
        }
    }

    private int getMessageWriteOffset(int topicNumber, int dataLength) throws IOException {
        topicWriteLocks[topicNumber].lock();
        int currentTopicMiniChunkNumber = dataFileIndexer.topicMiniChunkNumber[topicNumber];
        int currentTopicMiniChunkLength = dataFileIndexer.topicMiniChunkLengths[topicNumber][currentTopicMiniChunkNumber];

        // create 4MB mini chunk, if almost full or not exists
        if ((MINI_CHUNK_SIZE - currentTopicMiniChunkLength < dataLength) || topicMappedBuff[topicNumber] == null) {
            assignNextMiniChunk(topicNumber);
        }
        currentTopicMiniChunkNumber = dataFileIndexer.topicMiniChunkNumber[topicNumber];
        currentTopicMiniChunkLength = dataFileIndexer.topicMiniChunkLengths[topicNumber][currentTopicMiniChunkNumber];
        int offset = currentTopicMiniChunkLength;
        dataFileIndexer.topicMiniChunkLengths[topicNumber][currentTopicMiniChunkNumber] += dataLength;

        topicWriteLocks[topicNumber].unlock();
        return offset;
    }

    private void assignNextMiniChunk(int topicNumber) throws IOException {
        if (topicMappedBuff[topicNumber] != null) {
            // sync: which is optional, async also keeps correctness
//            topicMappedBuff[topicNumber].force();
            unmap(topicMappedBuff[topicNumber]);
        }

        // init 4MB mini chunk, set length 0
        dataFileIndexer.topicMiniChunkLengths[topicNumber][dataFileIndexer.topicMiniChunkNumber[topicNumber]] = 0;

        long miniChunkGlobalOffset = dataFileIndexer.topicOffsets[topicNumber] + MINI_CHUNK_SIZE * dataFileIndexer.topicMiniChunkNumber[topicNumber];
        topicMappedBuff[topicNumber] = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, miniChunkGlobalOffset, MINI_CHUNK_SIZE);

        dataFileIndexer.topicMiniChunkNumber[topicNumber]++;
    }

    static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
            //System.out.println("unmap successful");
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        for (int i = 0; i < dataFileIndexer.INIT_MAX_TOPIC_NUMBER; i++) {
            if (topicMappedBuff[i] != null) {
                unmap(topicMappedBuff[i]);
            }
        }

        int finished = numberOfFinished.incrementAndGet();
        if (finished == numberOfProducer.get()) {
            dataFile.close();
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(rootPath + File.separator + "index.bin")));
            oos.writeObject(dataFileIndexer);
            oos.close();
        }
    }
}

