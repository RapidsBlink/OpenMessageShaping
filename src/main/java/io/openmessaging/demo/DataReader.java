package io.openmessaging.demo;

import java.io.*;

import io.openmessaging.demo.DataFileIndexer;

/**
 * Created by will on 31/5/2017.
 */
public class DataReader {

    public DataReader(String rootFilePath) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(rootFilePath + File.separator + "index.bin"));
        DataFileIndexer myObject = (DataFileIndexer) ois.readObject();
    }
}
