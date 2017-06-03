package io.openmessaging.demo;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 5/31/17.
 */
public class DataFileIndexer implements Serializable {
    public int INIT_MAX_TOPIC_NUMBER = 200;
    public int TOPIC_CHUNK_SIZE = 100 * 1024 * 1024; // 100 MB
    // just for testing
    //public int TOPIC_CHUNK_SIZE = 500 * 1024 * 1024; // 400 MB

    public int MINI_CHUNK_SIZE = 8 * 1024 * 1024; // 4MB
    public int MAX_MINI_CHUNK_NUMBER_PER_TOPIC = TOPIC_CHUNK_SIZE / MINI_CHUNK_SIZE; //20

    // for quickly look topic chunk idx from topic name, used in production phase
    public ConcurrentHashMap<String, Integer> topicNameToNumber = new ConcurrentHashMap<>(INIT_MAX_TOPIC_NUMBER);
    // for quickly look up topic names from chunk idx, used in consumption phase
    public String[] topicNames = new String[INIT_MAX_TOPIC_NUMBER];

    public long[] topicOffsets = new long[INIT_MAX_TOPIC_NUMBER];

    // each 80MB, valid mini chunk number, used in consumption phase
    public int[] topicMiniChunkCurrMaxIndex = new int[INIT_MAX_TOPIC_NUMBER];
    // each 4MB size, valid content, used in consumption phase
    public int[][] topicMiniChunkLengths = new int[INIT_MAX_TOPIC_NUMBER][];
    //mini chunk start offset within the current topic chunk
    public int[][] topicMiniChunkStartOffset = new int[INIT_MAX_TOPIC_NUMBER][];

    public long currentGlobalDataOffset = 0;
    // serve as index also
    public int currentTopicNumber = 0;

    public ReentrantLock assignLock = new ReentrantLock();

    public DataFileIndexer() {
        topicNameToNumber.clear();

        for (int i = 0; i < INIT_MAX_TOPIC_NUMBER; i++) {
            topicOffsets[i] = 0L;
            topicMiniChunkCurrMaxIndex[i] = -1;
            topicMiniChunkLengths[i] = new int[MAX_MINI_CHUNK_NUMBER_PER_TOPIC];
            topicMiniChunkStartOffset[i] = new int[MAX_MINI_CHUNK_NUMBER_PER_TOPIC];
        }
    }

    private void assignNumberToTopic(String topicName) {
        assignLock.lock();
        if (!topicNameToNumber.containsKey(topicName)) {
            // currentTopicNumber serve as index
            topicNameToNumber.put(topicName, currentTopicNumber);
            topicOffsets[currentTopicNumber] = currentGlobalDataOffset;
            topicNames[currentTopicNumber] = topicName;

            currentGlobalDataOffset += TOPIC_CHUNK_SIZE;
            currentTopicNumber++;
        }
        assignLock.unlock();
    }

    int getAssignedTopicNumber(String topicName) {
        if (!topicNameToNumber.containsKey(topicName)) {
            assignNumberToTopic(topicName);
        }
        return topicNameToNumber.get(topicName);
    }
}
