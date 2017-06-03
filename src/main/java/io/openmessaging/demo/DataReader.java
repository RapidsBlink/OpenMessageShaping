package io.openmessaging.demo;


import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import io.openmessaging.Message;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

/**
 * Created by will on 26/5/2017.
 */

//5GB 90个topic，每个topic有10-20个线程，每个文件比较小，所以一次性读取整个文件到一个arraylist
public class DataReader {
    HashMap<String, ArrayList<File>> topicFileList;
    int fileIndex = 0;

    MessageDeserialization messageDeserialization = new MessageDeserialization();

    ArrayList<String> myToplicList = new ArrayList<>();
    int currentTopicIndex = -1;
    int innerTopicFileIndex = 0;
    int currentMessageIndex = 0;

    ArrayList<Message> currentFileContent = new ArrayList<>();
    //2MB read buff
    byte[] readBuff = new byte[2 * 1024 * 1024];

    boolean hasNextFlag = true;


    public DataReader(String fullRootPath) {
        topicFileList = new HashMap<String, ArrayList<File>>();
        File[] directories = new File(fullRootPath).listFiles(File::isDirectory);
        for (int i = 0; i < directories.length; i++) {
            File dir = directories[i];

            ArrayList<File> fileList = new ArrayList<File>();
            File[] allFiles = new File(fullRootPath + File.separator + dir.getName()).listFiles(File::isFile);
            for (int j = 0; j < allFiles.length; j++) {
                fileList.add(allFiles[j]);
            }
            topicFileList.put(dir.getName(), fileList);
        }

    }

    public void attachTopics(String queueName, Collection<String> nameList) {
        myToplicList.addAll(nameList);
        myToplicList.add(queueName);
        Collections.sort(myToplicList);
    }

    public Message fetchNextMessage() {
        boolean hasNext = true;
        if (currentMessageIndex == currentFileContent.size()) {
            hasNext = readNextTopicFile();
        }
        if (hasNext)
            return currentFileContent.get(currentMessageIndex++);
        else
            return null;
    }

    public boolean readNextTopicFile() {
        //finish one topic
        if (currentTopicIndex < 0 || innerTopicFileIndex == topicFileList.get(myToplicList.get(currentTopicIndex)).size()) {
            innerTopicFileIndex = 0;
            currentTopicIndex++;
            //finished all topics
            if (currentTopicIndex == myToplicList.size()) {
                return false;
            }
        }
        File nextFile = topicFileList.get(myToplicList.get(currentTopicIndex)).get(innerTopicFileIndex);
        currentFileContent.clear();
        try {
            GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(nextFile));
            BufferedInputStream bis = new BufferedInputStream(gzipInputStream);
            //init 8MB
            ByteArrayOutputStream bos = new ByteArrayOutputStream(4 * 1024 * 1024);
            int len;
            while ((len = bis.read(readBuff)) > 0) {
                bos.write(readBuff, 0, len);
            }
            ByteBuffer fileContent = ByteBuffer.wrap(bos.toByteArray());
            while (fileContent.position() < fileContent.limit()) {
                int messageLength = fileContent.getInt();
                for (int idx = 0; idx < messageLength; idx++) {
                    readBuff[idx] = fileContent.get();
                }
                currentFileContent.add(messageDeserialization.deserialize(readBuff, 0, messageLength));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        currentMessageIndex = 0;
        innerTopicFileIndex++;
        return true;

    }
}