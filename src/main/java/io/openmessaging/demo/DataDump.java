package io.openmessaging.demo;

import io.openmessaging.demo.net.jpountz.lz4.LZ4BlockOutputStream;
import io.openmessaging.demo.net.jpountz.lz4.LZ4JavaUnsafeCompressor;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by will on 25/5/2017.
 */


public class DataDump {
    private static Set<String> folderSet = new HashSet<>();
    private static ReentrantReadWriteLock folderRWLock = new ReentrantReadWriteLock();

    private final String rootPath;
    private HashMap<String, BufferedWriter> myFileName;

    public DataDump(String folderRootPath) {
        rootPath = folderRootPath;
        myFileName = new HashMap<>();
    }

    public void writeToFile(String topicName, DefaultBytesMessage message) {
        if (!myFileName.containsKey(topicName)) {
            createFile(topicName);
        }
        BufferedWriter bw = myFileName.get(topicName);
        try {
            bw.write(message.toString());
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
            LZ4BlockOutputStream zip = new LZ4BlockOutputStream(new FileOutputStream(
                    new File(rootPath + File.separator + folderName + File.separator + fileName)), 16 * 1024, new LZ4JavaUnsafeCompressor());
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
