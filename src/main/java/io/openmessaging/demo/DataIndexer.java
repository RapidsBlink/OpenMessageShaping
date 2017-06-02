package io.openmessaging.demo;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by yche on 6/2/17.
 */
public class DataIndexer implements Serializable {
    public ArrayList<Integer> mmapChunkLengthList = new ArrayList<>();

    public ArrayList<Integer> messageBytesLengthList=new ArrayList<>();
}
