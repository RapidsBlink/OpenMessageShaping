package io.openmessaging.demo;

import io.openmessaging.MessageHeader;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Created by yche on 5/27/17.
 */
public class NaiveDataReader implements Iterator<DefaultBytesMessage> {
    private final MessageDeserialization messageDeserialization;
    private final Base64.Decoder baseDec;
    private final String storePath;

    // 1st-level all folders
    private ArrayList<File[]> folderFilesList;
    private int folderIndex;

    // 2nd-level one folder
    private File[] files;
    private int fileIndex;

    // 3rd-level one file
//    private final static int BUFFER_SIZE = 4 * 1024 * 1024;
    private DataInputStream bufferedReader;

    private int lenBytes = 0;
    private byte[] tmpBinString = new byte[260 * 1024];
    private ByteBuffer byteBuffer = ByteBuffer.allocate(5 * 1024 * 1024);
    private int curBytesLen = 0;
//    private byte[] myLenBytes = new byte[4];

    public NaiveDataReader(String storePath) {
        this.storePath = storePath;
        this.messageDeserialization = new MessageDeserialization();
        this.baseDec = Base64.getDecoder();
    }

    private void fetchNextFolder() {
        files = folderFilesList.get(folderIndex);
        fileIndex = 0;
    }

    private void fetchNextFile() throws FileNotFoundException {
        try {
            GZIPInputStream zip = new GZIPInputStream(new BufferedInputStream(new FileInputStream(files[fileIndex])));
            bufferedReader = new DataInputStream(zip);
//            bufferedReader = new BufferedReader(new InputStreamReader(zip));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void fetchData() {
        try {
            lenBytes = bufferedReader.readInt();
            System.out.println(lenBytes);
            if (bufferedReader.available() == 0) {
                System.out.println("end shit!");
            }

            bufferedReader.read(tmpBinString, 0, lenBytes);
            curBytesLen += lenBytes + Integer.BYTES;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void attachNames(String queueName, Collection<String> topicNameList) {
        // folder_list
        ArrayList<String> folderNameList;
        folderNameList = new ArrayList<>(topicNameList.size() + 1);
        folderNameList.addAll(topicNameList);
        Collections.sort(folderNameList);

        folderNameList.add(queueName);

        // 1st-level: all folders
        folderFilesList = new ArrayList<>(folderNameList.size());
        folderNameList.forEach((folderString) -> {
            File[] files = new File(storePath + File.separator + folderString).listFiles(File::isFile);
            folderFilesList.add(files);
        });
        folderIndex = 0;

        // 2nd-level: one folder
        fetchNextFolder();

        // 3rd-level: one file
        try {
            fetchNextFile();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        fetchData();
    }

    private void nextAndUpdateIteratorStates() throws IOException {

        // 3rd-level: reach end of file
        if (curBytesLen >= files[fileIndex].length()) {
            curBytesLen = 0;
            // 2nd-level: reach end of files in this folder
            fileIndex++;
            if (fileIndex >= files.length) {
                folderIndex++;

                // 1st-level: go through all the files already
                if (folderIndex >= folderFilesList.size()) {
                    return;
                }
                files = folderFilesList.get(folderIndex);
                fileIndex = 0;
            }
            fetchNextFile();
        }
        fetchData();
    }

    @Override
    public boolean hasNext() {
        return folderIndex < folderFilesList.size();
    }

    @Override
    public DefaultBytesMessage next() {
        DefaultBytesMessage message = messageDeserialization.deserialize(tmpBinString, 0, lenBytes);
        System.out.println(message.headers().getString(MessageHeader.TOPIC) != null ? message.headers().getString(MessageHeader.TOPIC) : message.headers().getString(MessageHeader.QUEUE));
        System.out.println(new String(message.getBody()));
        try {
            nextAndUpdateIteratorStates();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return message;
    }
}