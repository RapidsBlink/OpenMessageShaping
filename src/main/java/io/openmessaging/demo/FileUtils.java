package io.openmessaging.demo;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

/**
 * Created by yche on 6/2/17.
 */
public class FileUtils {
    public static final int CHUNK_SIZE = 128 * 1024 * 1024;
    public static final long MAX_FILE_SIZE = 10L * 1024 * 1024 * 1024;

    public static void unmap(MappedByteBuffer mbb) {
        try {
            Method cleaner = mbb.getClass().getMethod("cleaner");
            cleaner.setAccessible(true);
            Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
            clean.invoke(cleaner.invoke(mbb));
        } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }
}
