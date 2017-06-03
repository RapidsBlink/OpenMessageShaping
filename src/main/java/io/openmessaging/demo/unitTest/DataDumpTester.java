package io.openmessaging.demo.unitTest;


import io.openmessaging.demo.DataDump;

import java.util.ArrayList;
import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by will on 25/5/2017.
 */


public class DataDumpTester {
    private static Logger LOGGER = Logger.getLogger("InfoLogging");

    public static void test0() {
        ArrayList<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            threads.add(new WorkerWriter());
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

        //MessageDeserialization
        DataReader dr = new DataReader("/tmp/test");
        ArrayList<String> topics = new ArrayList<>();
        topics.add("TOPIC0");
        topics.add("TOPIC1");
        topics.add("TOPIC2");
        topics.add("TOPIC3");
        topics.add("TOPIC4");
        for (int i = 0; i < 5; i++) {
            String topic = topics.get(i);
            ArrayList<byte[]> contents = dr.readTopic(topic);

            int correct = 0;
            boolean OK = true;
            for (byte[] messageBinary : contents) {
                for (int j = 1; j < messageBinary.length; j++) {
                    if (messageBinary[j] != (byte) (messageBinary[0] + j)) {
                        OK = false;
                        break;
                    }
                }
                if (!OK)
                    break;
                correct++;
            }
            LOGGER.info(Boolean.toString(OK) + ",correct:" + correct + ", all:" + contents.size());
        }
    }

    public static void main(String[] args) {
        test0();
    }

    static class WorkerWriter extends Thread {
        ArrayList<String> topics = new ArrayList<>();
        DataDump dd = new DataDump("/tmp/test");

        public WorkerWriter() {
            topics.add("TOPIC0");
            topics.add("TOPIC1");
            topics.add("TOPIC2");
            topics.add("TOPIC3");
            topics.add("TOPIC4");
        }

        public void run() {
            Random rnd = new Random();
            for (int i = 0; i < 100000; i++) {
                String topic = topics.get(rnd.nextInt(5));
                int length = 100;

                //TODO: MessageSerialization bug when we have length equals to 256*1024
                if (i % 1000 == 0) {
                    length = 256 * 1024;
                }

                byte[] input = new byte[length];
                input[0] = (byte) (i % 128);
                for (int j = 1; j < length; j++) {
                    input[j] = (byte) (j + input[0]);
                }
                dd.writeToFile(topic, input);
            }
            dd.close();
        }
    }
}
