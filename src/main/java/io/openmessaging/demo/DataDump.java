package io.openmessaging.demo;

import java.io.*;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
            GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(new File(rootPath + File.separator + folderName + File.separator + fileName)));
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zip));
            myFileName.put(folderName, writer);
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
