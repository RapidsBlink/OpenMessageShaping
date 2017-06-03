package io.openmessaging.demo;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * Created by will on 25/5/2017.
 */


public class DataDump {
    private static Set<String> folderSet;
    private static ReentrantReadWriteLock folderRWLock;

    static {
        folderRWLock = new ReentrantReadWriteLock();
        folderSet = new HashSet<>();
    }

    private final String rootPath;
    //    private final Base64.Encoder base64Encoder;
    private HashMap<String, DataOutputStream> myFileName;
    private HashMap<String, GZIPOutputStream> gzipFileName;

    public DataDump(String folderRootPath) {
        rootPath = folderRootPath;
        myFileName = new HashMap<>();
//        base64Encoder = Base64.getEncoder();
    }

    public void writeToFile(String topicName, byte[] data) {
        if (!myFileName.containsKey(topicName)) {
            createFile(topicName);
        }
        DataOutputStream bw = myFileName.get(topicName);
        try {
            bw.writeInt(data.length);
            bw.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createFile(String folderName) {
        folderRWLock.readLock().lock();
        if (!folderSet.contains(folderName)) {
            folderRWLock.readLock().unlock();
            createFolder(folderName);
        } else {
            folderRWLock.readLock().unlock();
        }

        String fileName = Thread.currentThread().getName();

        try {
            MyGZIPOutputStream zip = new MyGZIPOutputStream(new FileOutputStream(new File(rootPath + File.separator + folderName + File.separator + fileName)));
            zip.setLevel(Deflater.BEST_SPEED);
            DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(zip));
            myFileName.put(folderName, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {

        myFileName.forEach((topicName, bw) -> {
            try {
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        gzipFileName.forEach((topicName, bw) -> {
            try {
                bw.finish();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        myFileName.forEach((topicName, bw) -> {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void createFolder(String folderName) {
        folderRWLock.writeLock().lock();

        File dir = new File(rootPath + File.separator + folderName);
        if (!dir.exists()) {
            dir.mkdir();
        }
        folderSet.add(folderName);

        folderRWLock.writeLock().unlock();
    }
}
