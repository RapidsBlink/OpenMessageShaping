package io.openmessaging.demo;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Created by will on 31/5/2017.
 */
public class DataReader {
    //private static Logger LOGGER = Logger.getLogger("InfoLogging");

    static int BUFFER_NUMBER = 1;

    static RandomAccessFile dataFile;
    static FileChannel dataFileChannel;

    static AtomicInteger numberOfConsumer = new AtomicInteger(0);
    static DataFileIndexer dataFileIndexer;
    static ArrayList<String> topicsReverseOrderInDataFile = new ArrayList<>();

    //static ReentrantReadWriteLock topicBuffRWLock = new ReentrantReadWriteLock();
    static ConcurrentHashMap<String, Integer> topicBuff = new ConcurrentHashMap<>();
    public static HashMap<String, AtomicInteger> topicWaiterNumber = new HashMap<>();
    public static ReentrantLock topicWaiterNumberLock = new ReentrantLock();

    private static MappedByteBuffer[] bbuf = new MappedByteBuffer[BUFFER_NUMBER];
    private static AtomicInteger[] bbufFinishedTimes = new AtomicInteger[BUFFER_NUMBER];
    private static ArrayList<DefaultBytesMessage>[] topicMessageList = new ArrayList[BUFFER_NUMBER];

    static ReentrantLock hasNewDataBlockLoaded = new ReentrantLock();
    static Condition hasNewDataBlockLoadedCondition = hasNewDataBlockLoaded.newCondition();

    static boolean isInit = false;
    static ReentrantLock initLock = new ReentrantLock();

    static AtomicInteger currentChunkNum = new AtomicInteger();

    static ByteBuffer messageBinary = ByteBuffer.allocate(260 * 1024);
    static MessageDeserialization messageDeserialization = new MessageDeserialization();

    static AtomicInteger consumerFinishedAttached = new AtomicInteger(10);


    public DataReader(String rootFilePath) {
        initLock.lock();
        if (!isInit) {
            isInit = true;
            try {
                ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(rootFilePath + File.separator + "index.bin"));
                dataFileIndexer = (DataFileIndexer) ois.readObject();

                //currentChunkNum.set(dataFileIndexer.currentTopicNumber - 1);
                currentChunkNum.set(0);

                dataFile = new RandomAccessFile(rootFilePath + File.separator + "data.bin", "rw");
                dataFileChannel = dataFile.getChannel();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            for (int i = 0; i < BUFFER_NUMBER; i++) {
                bbufFinishedTimes[i] = new AtomicInteger(0);
            }

            for (int i = 0; i < dataFileIndexer.currentTopicNumber; i++) {
                topicsReverseOrderInDataFile.add(dataFileIndexer.topicNames[i]);
            }
        }
        initLock.unlock();

        int myRank = numberOfConsumer.getAndIncrement();
        System.out.println("Number of Consumer: " + (myRank + 1));
    }

    private void transformMappedBufferToMessageList(int topicBuffNumber, int globalTopicChunkNumber) {
        if (topicMessageList[topicBuffNumber] == null)
            topicMessageList[topicBuffNumber] = new ArrayList<>();
        topicMessageList[topicBuffNumber].clear();

        MappedByteBuffer buf = bbuf[topicBuffNumber];
        for (int miniChunkNum = 0; miniChunkNum <= dataFileIndexer.topicMiniChunkCurrMaxIndex[globalTopicChunkNumber]; miniChunkNum++) {
            int currentOffset = 0;
            int miniChunkOffset = dataFileIndexer.MINI_CHUNK_SIZE * miniChunkNum;
            buf.position(miniChunkOffset);
            while (true) {
                int dataLength = buf.getInt();
                messageBinary.clear();
                for (int idx = 0; idx < dataLength; idx++) {
                    messageBinary.put(buf.get());
                }
                messageBinary.flip();
                topicMessageList[topicBuffNumber].add(messageDeserialization.deserialize(messageBinary));
                currentOffset += dataLength + Integer.BYTES;
                if (currentOffset >= dataFileIndexer.topicMiniChunkLengths[globalTopicChunkNumber][miniChunkNum])
                    break;
            }
        }
    }

    public MappedByteBuffer getBufferedTopic(String topicName) {
        //LOGGER.info("Fetching " + topicName);
        //topicBuffRWLock.readLock().lock();
        boolean hasLoadedTopic = topicBuff.containsKey(topicName);
        //topicBuffRWLock.readLock().unlock();
        hasNewDataBlockLoaded.lock();
        try {
            while (!hasLoadedTopic) {
                //System.out.println(topicBuff.containsKey(topicName));
                hasNewDataBlockLoadedCondition.await();
                hasLoadedTopic = topicBuff.containsKey(topicName);

            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            hasNewDataBlockLoaded.unlock();
        }
        //here we can ensure data has been loaded
        //topicBuffRWLock.readLock().lock();
        int topicBuffIndex = topicBuff.get(topicName);
        //topicBuffRWLock.readLock().unlock();
        return bbuf[topicBuffIndex];
    }

    public ArrayList<DefaultBytesMessage> getTopicArrayList(String topicName) {
        //LOGGER.info("Fetching " + topicName);
        //topicBuffRWLock.readLock().lock();
        boolean hasLoadedTopic = topicBuff.containsKey(topicName);
        //topicBuffRWLock.readLock().unlock();
        hasNewDataBlockLoaded.lock();
        try {
            while (!hasLoadedTopic) {
                //LOGGER.info("NOT FOUND: " + topicName);

                hasNewDataBlockLoadedCondition.await();
                //LOGGER.info("Awake, check topic: " + topicName);

                //topicBuffRWLock.readLock().lock();
                hasLoadedTopic = topicBuff.containsKey(topicName);
                //LOGGER.info("has loadedTopic " + hasLoadedTopic);
                //topicBuffRWLock.readLock().unlock();
            }
            //LOGGER.info("Exit While...");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            //LOGGER.info("unlock hasNewDataBlockLoaded Lock..");
            hasNewDataBlockLoaded.unlock();
        }

        //here we can ensure data has been loaded
        //topicBuffRWLock.readLock().lock();
        int topicBuffIndex = topicBuff.get(topicName);
        //LOGGER.info(topicName + " topicBuffIndex " + topicBuffIndex);
        //topicBuffRWLock.readLock().unlock();
        return topicMessageList[topicBuffIndex];
    }

    public void countTopicListenerNumber(ArrayList<String> nameList) {
        for (String topic : nameList) {
            topicWaiterNumberLock.lock();
            if (!topicWaiterNumber.containsKey(topic)) {
                topicWaiterNumber.put(topic, new AtomicInteger(0));
            }
            topicWaiterNumber.get(topic).incrementAndGet();
            topicWaiterNumberLock.unlock();
        }
        int myRank = consumerFinishedAttached.decrementAndGet();
        if (myRank == 0) {
            String firstLoadTopicChunk = dataFileIndexer.topicNames[currentChunkNum.get()];
            while (!topicWaiterNumber.containsKey(firstLoadTopicChunk) || topicWaiterNumber.get(firstLoadTopicChunk).get() == 0) {
                firstLoadTopicChunk = dataFileIndexer.topicNames[currentChunkNum.getAndIncrement()];
            }
            loadOneTopicChunk(0);
        }
    }

    private void loadOneTopicChunk(int topicBuffNumber) {
        //load next topic chunk
//        if (currentChunkNum.get() < 0) {
//            LOGGER.info("All chunk finished. exit loader..");
//            return;
//        }
        //LOGGER.info("load " + dataFileIndexer.topicNames[currentChunkNum.get()]);
        int globalTopicChunkNumber = currentChunkNum.getAndIncrement();
        if (globalTopicChunkNumber >= dataFileIndexer.currentTopicNumber) return;
        while (topicWaiterNumber.get(dataFileIndexer.topicNames[globalTopicChunkNumber]) == null ||
                topicWaiterNumber.get(dataFileIndexer.topicNames[globalTopicChunkNumber]).get() == 0) {
            globalTopicChunkNumber = currentChunkNum.getAndIncrement();
            if (globalTopicChunkNumber >= dataFileIndexer.currentTopicNumber) return;
        }

        try {
            bbuf[topicBuffNumber] = dataFileChannel.map(FileChannel.MapMode.READ_ONLY,
                    dataFileIndexer.topicOffsets[globalTopicChunkNumber], dataFileIndexer.TOPIC_CHUNK_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        bbuf[topicBuffNumber].load();
        transformMappedBufferToMessageList(topicBuffNumber, globalTopicChunkNumber);
        bbufFinishedTimes[topicBuffNumber].set(0);
        //topicBuffRWLock.writeLock().lock();
        topicBuff.put(dataFileIndexer.topicNames[globalTopicChunkNumber], topicBuffNumber);
        //topicBuffRWLock.writeLock().unlock();
        hasNewDataBlockLoaded.lock();
        hasNewDataBlockLoadedCondition.signalAll();
        hasNewDataBlockLoaded.unlock();
    }

    public void finishedTopic(String topicName) {

        int topicBuffNum = topicBuff.get(topicName);
        int finished = topicWaiterNumber.get(topicName).decrementAndGet();
        //int finished = bbufFinishedTimes[topicBuffNum].incrementAndGet();
        //the last one consumer on this topic
        if (finished == 0) {
            //LOGGER.info("finished " + topicName);
            DataDumper.unmap(bbuf[topicBuffNum]);
            loadOneTopicChunk(topicBuffNum);
        }
    }

    public ArrayList topicsReverseOrderInDataFile() {
        return DataReader.topicsReverseOrderInDataFile;
    }

    public DataFileIndexer getDataFileIndexer() {
        return DataReader.dataFileIndexer;
    }
}
