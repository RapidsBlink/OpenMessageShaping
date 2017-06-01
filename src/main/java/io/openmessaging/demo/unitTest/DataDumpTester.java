package io.openmessaging.demo.unitTest;

import io.openmessaging.demo.DataDumper;
import io.openmessaging.demo.DataReader;
import io.openmessaging.demo.DataFileIndexer;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by will on 31/5/2017.
 */

class Worker extends Thread {
    ArrayList<String> topics = new ArrayList<>();
    DataDumper dd = new DataDumper("/tmp/test");

    public Worker() throws IOException {
        for(int i = 0 ; i < 70; i++){
            topics.add("TOPIC" + i);
        }
    }

    public void run() {
        Random rnd = new Random();
        for (int i = 0; i < 4000000; i++) {
            //String topic = topics.get(rnd.nextInt(5));
            String topic = topics.get(i%70);
            int length = 100;
//            if (i % 1000 == 0) {
//                length = 256 * 1024;
//            }

            byte[] input = new byte[length];
            input[0] = (byte)i;
            for (int j = 1; j < length; j++) {
                input[j] = (byte) ((input[0] + j) % 128);
            }
            try {
                dd.writeToFile(topic, input);
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

public class DataDumpTester {
    private static Logger LOGGER = Logger.getLogger("InfoLogging");

    static DataDumper dataDumper;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        new DataDumpTester().dataDumpTestWithSingleThreadReader();
    }

    public void dataDumpTestWithSingleThreadReader() throws IOException, ClassNotFoundException, InterruptedException {
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            threads.add(new Worker());
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

        LOGGER.info("DataDump Finished.");

        //MessageDeserialization
        DataReader dr = new DataReader("/tmp/test");
        DataFileIndexer dataFileIndexer = dr.getDataFileIndexer();
        ArrayList<String> topics = dr.topicsReverseOrderInDataFile();
        for (int i = 0; i < topics.size(); i++) {
            String topic = topics.get(i);
            ArrayList<byte[]> contents = new ArrayList<>();
            int topicNumber = dataFileIndexer.topicNameToNumber.get(topic);
            MappedByteBuffer buf = dr.getBufferedTopic(topic);
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
                    contents.add(data);
                    currentOffset += dataLength + Integer.BYTES;
                    if (currentOffset >= dataFileIndexer.topicMiniChunkLengths[topicNumber][miniChunkNum])
                        break;
                }
            }
            int correct = 0;
            boolean OK = true;
            for (byte[] messageBinary : contents) {
                for (int j = 1; j < messageBinary.length; j++) {
                    if (messageBinary[j] != (byte)((messageBinary[0] + j) % 128)) {
                        OK = false;
                        break;
                    }
                }
                if (!OK)
                    break;
                correct++;
            }
            LOGGER.info(Boolean.toString(OK) + ",correct:" + correct + ", all:" + contents.size());
            dr.finishedTopic(topic);
        }
    }

}
