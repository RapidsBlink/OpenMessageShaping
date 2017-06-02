package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Created by will on 31/5/2017.
 */
public class DataReader {
    private static int CHUNK_SIZE = 128 * 1024 * 1024;
    private static boolean isInit = false;
    private static ReentrantLock initLock = new ReentrantLock();
    private static RandomAccessFile dataFile;
    private static PullConsumer initConsumer = null;

    private static FileChannel dataFileChannel;
    private static ArrayList<Integer> mmapChunkLengthList;
    private static ArrayList<DefaultBytesMessage> defaultBytesMessageArrayList = new ArrayList<>();

    int nextChunkIndex = 0;
    private boolean isEnd = false;

    private static AtomicInteger numberOfConsumer = new AtomicInteger(0);
    private static AtomicInteger numberOfActiveConsumer = new AtomicInteger(0);

    private static ByteBuffer messageBinary = ByteBuffer.allocate(260 * 1024);
    private static MessageDeserialization messageDeserialization = new MessageDeserialization();

    private static boolean isReady = false;
    private static ReentrantLock isReadyUpdate = new ReentrantLock();
    private static ReentrantLock isBulkSyncDataReadyLock = new ReentrantLock();
    private static Condition isBulkSyncDataReadyCond = isBulkSyncDataReadyLock.newCondition();

    public DataReader(String fileRootPath, PullConsumer pullConsumer) {
        initLock.lock();
        if (!isInit) {
            initConsumer = pullConsumer;
            isInit = true;
        }
        initLock.unlock();

        if (pullConsumer == initConsumer) {
            try {
                dataFile = new RandomAccessFile(fileRootPath + File.separator + "data.bin", "rw");
                dataFileChannel = dataFile.getChannel();
                ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fileRootPath + File.separator + "index.bin")));
                mmapChunkLengthList = ((DataIndexer) ois.readObject()).mmapChunkLengthList;
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            numberOfConsumer.incrementAndGet();
        }
    }

    public boolean isEnd() {
        return isEnd;
    }

    private void waitForMessageList() {
        isBulkSyncDataReadyLock.lock();
        while (!isReady) {
            try {
                isBulkSyncDataReadyCond.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        isBulkSyncDataReadyLock.unlock();
    }

    private void updateMessageList() {
        defaultBytesMessageArrayList.clear();
        try {
            MappedByteBuffer mappedByteBuffer = dataFileChannel.map(FileChannel.MapMode.READ_ONLY, CHUNK_SIZE * nextChunkIndex, mmapChunkLengthList.get(nextChunkIndex));
            mappedByteBuffer.load();
            while (mappedByteBuffer.hasRemaining()) {
                int dataLength = mappedByteBuffer.getInt();
                messageBinary.clear();
                for (int idx = 0; idx < dataLength; idx++) {
                    messageBinary.put(mappedByteBuffer.get());
                }
                messageBinary.flip();
                defaultBytesMessageArrayList.add(messageDeserialization.deserialize(messageBinary));
            }

            unmap(mappedByteBuffer);

            // busy waiting
            while (numberOfActiveConsumer.get() > 0) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            isBulkSyncDataReadyLock.lock();
            isBulkSyncDataReadyCond.signalAll();
            isBulkSyncDataReadyLock.unlock();
        } catch (IOException e) {
            e.printStackTrace();
        }
        nextChunkIndex++;
    }

    ArrayList<DefaultBytesMessage> requestNextChunkMessage(PullConsumer pullConsumer) {
        if (nextChunkIndex >= mmapChunkLengthList.size()) {
            isEnd = true;
            return null;
        } else {
            if (pullConsumer != initConsumer) {
                numberOfActiveConsumer.decrementAndGet();
                waitForMessageList();
                numberOfActiveConsumer.incrementAndGet();
            } else {
                // the thread to do most work
                updateMessageList();
            }
        }
        return defaultBytesMessageArrayList;
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
}
