package io.openmessaging.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    private final Base64.Encoder base64Encoder;
    private HashMap<String, BufferedWriter> myFileName;


    public DataDump(String folderRootPath) {
        rootPath = folderRootPath;
        myFileName = new HashMap<>();
        base64Encoder = Base64.getEncoder();
    }

    public void writeToFile(String topicName, byte[] data) {
        if (!myFileName.containsKey(topicName)) {
            createFile(topicName);
        }
        BufferedWriter bw = myFileName.get(topicName);
        try {
            bw.write(base64Encoder.encodeToString(data));
            bw.newLine();
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
            FileWriter myFile = new FileWriter(rootPath + File.separator + folderName + File.separator + fileName);
            BufferedWriter bw = new BufferedWriter(myFile);
            myFileName.put(folderName, bw);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
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
