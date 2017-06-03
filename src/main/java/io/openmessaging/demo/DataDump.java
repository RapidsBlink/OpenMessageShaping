package io.openmessaging.demo;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Base64;
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
    private final Base64.Encoder base64Encoder;
    private HashMap<String, GZIPOutputStream> myFileName;
    private ByteBuffer integerToBytes = ByteBuffer.allocate(4);

    public DataDump(String folderRootPath) {
        rootPath = folderRootPath;
        myFileName = new HashMap<>();
        base64Encoder = Base64.getEncoder();
    }

    public void writeToFile(String topicName, byte[] data) {
        if (!myFileName.containsKey(topicName)) {
            createFile(topicName);
        }
        GZIPOutputStream bw = myFileName.get(topicName);
        try {
            integerToBytes.clear();
            bw.write(integerToBytes.putInt(data.length).array());
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
            GZIPOutputStream zip = new GZIPOutputStream(new FileOutputStream(rootPath + File.separator + folderName + File.separator + fileName), 1 * 1024 * 1024);
            myFileName.put(folderName, zip);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        myFileName.forEach((topicName, bw) -> {
            try {
                bw.finish();
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
