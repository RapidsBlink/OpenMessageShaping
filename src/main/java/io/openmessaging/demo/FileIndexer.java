package io.openmessaging.demo;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by yche on 6/3/17.
 */
public class FileIndexer implements Serializable {
    ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicInteger>> fileSizeMap = new ConcurrentHashMap<>();

    private FileIndexer fileIndexer = new FileIndexer();

    public FileIndexer createFileIndexer() {
        return fileIndexer;
    }
}
