package io.openmessaging.demo.unitTest;

import io.openmessaging.MessageHeader;
import io.openmessaging.demo.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by will on 1/6/2017.
 */

class MessageWorker extends Thread {
    ArrayList<String> topics = new ArrayList<>();
    DataDumper dd = new DataDumper("/tmp/test");

    ByteBuffer messageBinary = ByteBuffer.allocate(260 * 1024);
    MessageSerialization messageSerialization = new MessageSerialization();

    public MessageWorker() throws IOException {
        for (int i = 0; i < 70; i++) {
            topics.add("TOPIC" + i);
        }
    }

    public void run() {
        Random rnd = new Random();
        for (int i = 0; i < 4000000; i++) {
            //String topic = topics.get(rnd.nextInt(5));
            String topic = topics.get(i % 70);
            int length = 80;
//            if (i % 1000 == 0) {
//                length = 256 * 1024;
//            }

            byte[] input = new byte[length];
            input[0] = (byte) (i % 128);
            for (int j = 1; j < length; j++) {
                input[j] = (byte) ((input[0] + j) % 128);
            }
            DefaultBytesMessage message = new DefaultBytesMessage(input);
            message.putHeaders(MessageHeader.TOPIC, topic);
            message.putProperties("id", i%128);
            messageBinary.clear();
            messageSerialization.serialize(message, messageBinary);
            messageBinary.flip();
            try {
                dd.writeToFile(topic, messageBinary);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            dd.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
class MessageReader extends Thread {
    private static Logger LOGGER = Logger.getLogger("InfoLogging");

    DataReader dr = new DataReader("/tmp/test");
    ByteBuffer messageBinary = ByteBuffer.allocate(260 * 1024);
    MessageDeserialization messageDeserialization = new MessageDeserialization();

    ArrayList<String> myNameList = new ArrayList<>();
    DataFileIndexer dataFileIndexer;

    MessageReader(ArrayList<String> nameList){
        ArrayList<String> dataFileOrderedTopics = dr.topicsReverseOrderInDataFile();
        dataFileIndexer = dr.getDataFileIndexer();
        for (String topic : dataFileOrderedTopics) {
            for (int i = 0; i < nameList.size(); i++) {
                if (nameList.get(i).equals(topic))
                    myNameList.add(topic);
            }
        }
        for(String topic:nameList){
            DataReader.topicWaiterNumberLock.lock();
            if(!DataReader.topicWaiterNumber.containsKey(topic)){
                DataReader.topicWaiterNumber.put(topic, new AtomicInteger(0));
            }
            DataReader.topicWaiterNumber.get(topic).incrementAndGet();
            DataReader.topicWaiterNumberLock.unlock();
        }
    }
    public void run() {
        for (String topic : myNameList) {
            ArrayList<DefaultBytesMessage> contents = new ArrayList<>();
            int topicNumber = dataFileIndexer.topicNameToNumber.get(topic);
            MappedByteBuffer buf = null;

            buf = dr.getBufferedTopic(topic);

            for (int miniChunkNum = 0; miniChunkNum <= dataFileIndexer.topicMiniChunkCurrMaxIndex[topicNumber]; miniChunkNum++) {
                int currentOffset = 0;
                int miniChunkOffset = dataFileIndexer.MINI_CHUNK_SIZE * miniChunkNum;
                buf.position(miniChunkOffset);
                while (true) {
                    int dataLength = buf.getInt();
                    byte[] data = new byte[dataLength];
                    for (int idx = 0; idx < dataLength; idx++) {
                        data[idx] = buf.get();
                    }
                    contents.add(messageDeserialization.deserialize(data));
                    currentOffset += dataLength + Integer.BYTES;
                    if (currentOffset >= dataFileIndexer.topicMiniChunkLengths[topicNumber][miniChunkNum])
                        break;
                }
            }
            int correct = 0;
            boolean OK = true;
            for (DefaultBytesMessage message : contents) {
                byte id = (byte)message.properties().getInt("id");
                byte body0 = message.getBody()[0];
                if(id != body0)
                    OK = false;
                if (!OK)
                    break;
                correct++;
            }
            LOGGER.info(Boolean.toString(OK) + ",correct:" + correct + ", all:" + contents.size());
            dr.finishedTopic(topic);
        }


    }
}
public class MessageDumpTester {
    private static Logger LOGGER = Logger.getLogger("InfoLogging");
    public static void main(String[] args) throws IOException {
        Logger applog = Logger.getGlobal();
        // Create and set handler
        Handler systemOut = new ConsoleHandler();
        systemOut.setLevel( Level.ALL );
        applog.addHandler( systemOut );
        applog.setLevel( Level.ALL );

        new MessageDumpTester().multiThreadsReader();
    }
    public void multiThreadsReader() throws IOException {
        long start = System.currentTimeMillis();
        multiThreadsWriter();
        long end = System.currentTimeMillis();
        LOGGER.info("Producer Finished, Cost " + (end - start) + " ms");

        LOGGER.info("Data dump finished");

        ArrayList<Thread> threads = new ArrayList<>();

        ArrayList<String> allTopics = new ArrayList<>();

        for (int i = 0; i < 70; i++) {
            allTopics.add("TOPIC" + i);
        }

        start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            threads.add(new MessageReader(new ArrayList<>(allTopics.subList(i * 7, (i+1)*7))));
        }
        for (int i = 0; i < 10; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < 10; i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        end = System.currentTimeMillis();
        LOGGER.info("Consumer Finished, Cost " + (end - start) + " ms");
    }
    public void multiThreadsWriter() throws IOException {
        ArrayList<Thread> threads = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            threads.add(new MessageWorker());
        }
        for (int i = 0; i < 10; i++) {
            threads.get(i).start();
        }
        for (int i = 0; i < 10; i++) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long end = System.currentTimeMillis();
        LOGGER.info("Consumer Finished, Cost " + (end - start) + " ms");
        LOGGER.info("DataDump Finished.");

    }

}
