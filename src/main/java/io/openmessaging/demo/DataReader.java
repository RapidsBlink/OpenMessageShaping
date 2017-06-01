package io.openmessaging.demo;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import io.openmessaging.demo.DataFileIndexer;

/**
 * Created by will on 31/5/2017.
 */
public class DataReader {
    private static Logger LOGGER = Logger.getLogger("InfoLogging");

    static int BUFFEREN_NUMBER = 1;
    static int TOPIC_NUMBER = 100;

    static RandomAccessFile dataFile;
    static FileChannel dataFileChannel;

    static AtomicInteger numberOfConsumer = new AtomicInteger(0);
    static AtomicInteger numberOfFinishedConsumer = new AtomicInteger(0);
    static DataFileIndexer dataFileIndexer;
    static ArrayList<String> topicsReverseOrderInDataFile = new ArrayList<>();
    static ConcurrentHashMap<String, Integer> topicBuff = new ConcurrentHashMap<>();
    public static HashMap<String, AtomicInteger> topicWaiterNumber = new HashMap<>();
    public static ReentrantLock topicWaiterNumberLock = new ReentrantLock();

    static MappedByteBuffer[] bbuf = new MappedByteBuffer[BUFFEREN_NUMBER];
    static AtomicInteger[] bbufFinishedTimes = new AtomicInteger[BUFFEREN_NUMBER];

    static ReentrantLock hasNewDataBlockLoaded = new ReentrantLock();
    static Condition hasNewDataBlockLoadedCondition = hasNewDataBlockLoaded.newCondition();

    static boolean isInit = false;
    static ReentrantLock initLock = new ReentrantLock();

    static AtomicInteger currentChunkNum = new AtomicInteger();


    public DataReader(String rootFilePath){
        initLock.lock();
        if (!isInit) {
            isInit = true;
            try {
                ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(rootFilePath + File.separator + "index.bin"));
                dataFileIndexer = (DataFileIndexer) ois.readObject();

                currentChunkNum.set(dataFileIndexer.currentTopicNumber - 1);

                dataFile = new RandomAccessFile(rootFilePath + File.separator + "data.bin", "rw");
                dataFileChannel = dataFile.getChannel();
            }catch (IOException e){
                e.printStackTrace();
            }catch (ClassNotFoundException e){
                e.printStackTrace();
            }

            for(int i = 0 ; i < BUFFEREN_NUMBER; i++){
                bbufFinishedTimes[i] = new AtomicInteger(0);
            }

            for (int i = dataFileIndexer.currentTopicNumber - 1; i >= 0; i--) {
                topicsReverseOrderInDataFile.add(dataFileIndexer.topicNames[i]);
            }
        }
        initLock.unlock();
        int myRank = numberOfConsumer.getAndIncrement();
        if (myRank < BUFFEREN_NUMBER && currentChunkNum.get() >= 0) {
            int myTopicNumber = currentChunkNum.getAndDecrement();
            try {
                bbuf[myRank] = dataFileChannel.map(FileChannel.MapMode.READ_ONLY,
                        dataFileIndexer.topicOffsets[myTopicNumber], dataFileIndexer.TOPIC_CHUNK_SIZE);
            }catch (IOException e){
                e.printStackTrace();
            }

            bbuf[myRank].load();
            bbufFinishedTimes[myRank].set(0);
            String topicName = dataFileIndexer.topicNames[myTopicNumber];
            LOGGER.info(topicName);
            topicBuff.put(topicName, myRank);
        }
    }

    public MappedByteBuffer getBufferedTopic(String topicName) {
        while (!topicBuff.containsKey(topicName)) {
            //System.out.println(topicBuff.containsKey(topicName));
            try {
                hasNewDataBlockLoaded.lock();
                hasNewDataBlockLoadedCondition.await();
            }catch (InterruptedException e){
                e.printStackTrace();
            }finally {
                hasNewDataBlockLoaded.unlock();
            }


        }
        //here we can ensure data has been loaded
        return bbuf[topicBuff.get(topicName)];
    }

    public void finishedTopic(String topicName) {
        int topicBuffNum = topicBuff.get(topicName);
        int finished = bbufFinishedTimes[topicBuffNum].incrementAndGet();
        //the last one consumer on this topic
        if (finished == topicWaiterNumber.get(topicName).get()) {
            bbufFinishedTimes[topicBuffNum].set(0);
            //load next topic chunk
            DataDumper.unmap(bbuf[topicBuffNum]);
            int globalTopicChunkNumber = currentChunkNum.getAndDecrement();
            if(globalTopicChunkNumber < 0) return;
            try {
                bbuf[topicBuffNum] = dataFileChannel.map(FileChannel.MapMode.READ_ONLY,
                        dataFileIndexer.topicOffsets[globalTopicChunkNumber], dataFileIndexer.TOPIC_CHUNK_SIZE);
            }catch (IOException e){
                e.printStackTrace();
            }

            bbuf[topicBuffNum].load();
            bbufFinishedTimes[topicBuffNum].set(0);
            topicBuff.put(dataFileIndexer.topicNames[globalTopicChunkNumber], topicBuffNum);
            hasNewDataBlockLoaded.lock();
            hasNewDataBlockLoadedCondition.signalAll();
            hasNewDataBlockLoaded.unlock();

        }
    }
    public ArrayList topicsReverseOrderInDataFile(){
        return DataReader.topicsReverseOrderInDataFile;
    }
    public DataFileIndexer getDataFileIndexer(){
        return DataReader.dataFileIndexer;
    }
    public void close() {

    }
}
