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
    private static String rootPath;
    private static long DATA_FILE_SIZE = 12L * 1024 * 1024 * 1024; // 8GB

    private static RandomAccessFile dataFile;
    private static FileChannel dataFileChannel;
    private static DataFileIndexer dataFileIndexer = new DataFileIndexer();
    private static int MINI_CHUNK_SIZE = dataFileIndexer.MINI_CHUNK_SIZE;
    private static int WASTE_SIZE = 256 * 1024;

    private static MappedByteBuffer[] topicMappedBuff = new MappedByteBuffer[dataFileIndexer.INIT_MAX_TOPIC_NUMBER];

    private static ReentrantLock[] topicWriteLocks = new ReentrantLock[dataFileIndexer.INIT_MAX_TOPIC_NUMBER];
    private static AtomicInteger[] mmapedAreaUserNumArr = new AtomicInteger[dataFileIndexer.INIT_MAX_TOPIC_NUMBER];

    private static AtomicInteger numberOfProducer = new AtomicInteger(0);
    private static AtomicInteger numberOfFinished = new AtomicInteger(0);

    private static boolean isInit = false;
    private static ReentrantLock initLock = new ReentrantLock();

    static byte[][] topicWriteBuff = new byte[dataFileIndexer.INIT_MAX_TOPIC_NUMBER][];
    static int[] topicWriteBuffLength = new int[dataFileIndexer.INIT_MAX_TOPIC_NUMBER];
    private ByteBuffer integerToByte = ByteBuffer.allocate(Integer.BYTES);

    //static ReentrantLock mmapUmapLock = new ReentrantLock();

    public DataDumper(String fileRootPath) throws IOException {
        initLock.lock();
        if (!isInit) {
            rootPath = fileRootPath;
            dataFile = new RandomAccessFile(fileRootPath + File.separator + "data.bin", "rw");
            dataFile.setLength(DATA_FILE_SIZE);
            dataFileChannel = dataFile.getChannel();
            for (int i = 0; i < dataFileIndexer.INIT_MAX_TOPIC_NUMBER; i++) {
                topicMappedBuff[i] = null;
                topicWriteBuff[i] = null;
                topicWriteLocks[i] = new ReentrantLock();
                mmapedAreaUserNumArr[i] = new AtomicInteger(0);
            }
            isInit = true;
        }
        initLock.unlock();

        numberOfProducer.incrementAndGet();
    }

    public void writeToFile(String topicName, byte[] data, int length) throws IOException {

        int topicNumber = dataFileIndexer.getAssignedTopicNumber(topicName);

        int offset = getMessageWriteOffset(topicNumber, length + Integer.BYTES);
        //int currentTopicMiniChunkIndex = dataFileIndexer.topicMiniChunkCurrMaxIndex[topicNumber];

        //MappedByteBuffer buf = topicMappedBuff[topicNumber];

        //System.out.println(offset + " topic: " + dataFileIndexer.topicNames[topicNumber] + " MiniChunkIndex:" + currentTopicMiniChunkIndex);
        integerToByte.clear();
        integerToByte.putInt(length);
        byte[] integerByte = integerToByte.array();
        for(int i = 0 ; i < Integer.BYTES; i++){
            topicWriteBuff[topicNumber][offset + i] = integerByte[i];
        }
        offset+= Integer.BYTES;
        for(int i = 0 ; i < length; i++){
            topicWriteBuff[topicNumber][offset + i] = data[i];
        }
        // 1st: length of byte arr
//        buf.putInt(offset, length);
//        offset += Integer.BYTES;
//        // 2nd: byte arr
//        for (int i = 0; i < length; i++) {
//            buf.put(offset + i, data[i]);
//        }

        // record worker num in this ares
        mmapedAreaUserNumArr[topicNumber].decrementAndGet();

//        if(currentUserNumber == 0 && currentTopicMiniChunkIndex < dataFileIndexer.topicMiniChunkCurrMaxIndex[topicNumber]){
//            unmap(buf);
//        }

    }

    public void writeToFile(String topicName, ByteBuffer data) throws IOException {
        byte[] dataBytes = data.array();
        writeToFile(topicName, dataBytes, data.limit());
    }


    private int getMessageWriteOffset(int topicNumber, int dataLength) throws IOException {
        topicWriteLocks[topicNumber].lock();
        int currentTopicMiniChunkNumber = dataFileIndexer.topicMiniChunkCurrMaxIndex[topicNumber];
        if (currentTopicMiniChunkNumber < 0) {
            assignNextMiniChunk(topicNumber);
        }
        int currentTopicMiniChunkLength = topicWriteBuffLength[topicNumber];

        // create 4MB mini chunk, if almost full or not exists
        if ((MINI_CHUNK_SIZE - currentTopicMiniChunkLength < WASTE_SIZE) || topicMappedBuff[topicNumber] == null) {
            assignNextMiniChunk(topicNumber);
        }

        // record worker num in this ares
        mmapedAreaUserNumArr[topicNumber].incrementAndGet();

        currentTopicMiniChunkLength = topicWriteBuffLength[topicNumber];
        int offset = currentTopicMiniChunkLength;
        topicWriteBuffLength[topicNumber] += dataLength;

        topicWriteLocks[topicNumber].unlock();

        return offset;
    }

    private int alignPageOffset(int topicNumber, int miniChunkIndex){
        int lastMiniChunkStartOffset = 0;
        int lastMiniChunkLength = 0;
        if(miniChunkIndex > 0) {
            lastMiniChunkStartOffset = dataFileIndexer.topicMiniChunkStartOffset[topicNumber][miniChunkIndex - 1];
            lastMiniChunkLength = dataFileIndexer.topicMiniChunkLengths[topicNumber][miniChunkIndex - 1];
        }
        int myStartOffset = lastMiniChunkStartOffset + lastMiniChunkLength;
        if((myStartOffset % 4096) != 0){
           myStartOffset =  (myStartOffset/4096+1) * 4096;
        }
        return myStartOffset;
    }

    private void passDataToOS(int topicNumber){
        try {
            byte[] compressedData = CompressionUtils.compress(topicWriteBuff[topicNumber], 0, topicWriteBuffLength[topicNumber]);

            int currentTopicMiniChunkIndex = dataFileIndexer.topicMiniChunkCurrMaxIndex[topicNumber];
            int currentTopicMiniChunkStartOffset = alignPageOffset(topicNumber, currentTopicMiniChunkIndex);
            long miniChunkGlobalOffset = dataFileIndexer.topicOffsets[topicNumber] + currentTopicMiniChunkStartOffset;
            topicMappedBuff[topicNumber] = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, miniChunkGlobalOffset, compressedData.length);
            topicMappedBuff[topicNumber].put(compressedData);

            dataFileIndexer.topicMiniChunkLengths[topicNumber][currentTopicMiniChunkIndex] = compressedData.length;
            dataFileIndexer.topicMiniChunkStartOffset[topicNumber][currentTopicMiniChunkIndex] = currentTopicMiniChunkStartOffset;

            unmap(topicMappedBuff[topicNumber]);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assignNextMiniChunk(int topicNumber) {
        if (topicWriteBuff[topicNumber] != null) {
            // sync: which is optional, async also keeps correctness
            // need to make sure no other threads use this mapped area, busy waiting here
            while (mmapedAreaUserNumArr[topicNumber].get() >= 0) {
                if (mmapedAreaUserNumArr[topicNumber].get() == 0) {
                    passDataToOS(topicNumber);
                    break;
                }
            }
        }else {
            topicWriteBuff[topicNumber] = new byte[MINI_CHUNK_SIZE];
        }
        dataFileIndexer.topicMiniChunkCurrMaxIndex[topicNumber]++;
        topicWriteBuffLength[topicNumber] = 0;

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

        int finished = numberOfFinished.incrementAndGet();
        if (finished == numberOfProducer.get()) {
            for (int i = 0; i < dataFileIndexer.INIT_MAX_TOPIC_NUMBER; i++) {
                if (topicWriteBuff[i] != null && dataFileIndexer.topicMiniChunkCurrMaxIndex[i] >= 0) {
                    passDataToOS(i);
                }
            }

            dataFile.close();
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(rootPath + File.separator + "index.bin")));
            oos.writeObject(dataFileIndexer);
            oos.close();
        }
    }
}

