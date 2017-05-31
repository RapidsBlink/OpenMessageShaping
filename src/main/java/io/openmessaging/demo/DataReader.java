package io.openmessaging.demo;

import java.io.*;
import java.nio.channels.FileChannel;

import io.openmessaging.demo.DataFileIndexer;

/**
 * Created by will on 31/5/2017.
 */
public class DataReader {

    static RandomAccessFile dataFile;
    static FileChannel dataFileChannel;
    public DataReader(String rootFilePath) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(rootFilePath + File.separator + "index.bin"));
        DataFileIndexer myObject = (DataFileIndexer) ois.readObject();

        dataFile = new RandomAccessFile(rootFilePath + File.separator + "data.bin", "rw");
        dataFileChannel = dataFile.getChannel();
    }




}
