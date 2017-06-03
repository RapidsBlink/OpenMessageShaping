package io.openmessaging.demo.unitTest;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;

/**
 * Created by will on 26/5/2017.
 */

//5GB 90个topic，每个topic有10-20个线程，每个文件比较小，所以一次性读取整个文件到一个arraylist
public class DataReader {
    HashMap<String, ArrayList<BufferedReader>> filesReaderedBuffer;
    ArrayList<byte[]> fileContents;
    int fileIndex = 0;
    Base64.Decoder baseDec = Base64.getDecoder();

    public DataReader(String fullRootPath) {
        filesReaderedBuffer = new HashMap<String, ArrayList<BufferedReader>>();
        File[] directories = new File(fullRootPath).listFiles(File::isDirectory);
        for (int i = 0; i < directories.length; i++) {
            File dir = directories[i];

            ArrayList<BufferedReader> filesRB = new ArrayList<BufferedReader>();
            File[] fileLists = new File(fullRootPath + File.separator + dir.getName()).listFiles(File::isFile);
            for (int j = 0; j < fileLists.length; j++) {
                try {
                    FileReader fr = new FileReader(fullRootPath + File.separator + dir.getName() + File.separator + fileLists[j].getName());
                    filesRB.add(new BufferedReader(fr));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            filesReaderedBuffer.put(dir.getName(), filesRB);
        }
    }


    public ArrayList<byte[]> readTopic(String topicName) {
        ArrayList<byte[]> result = new ArrayList<>();

        ArrayList<BufferedReader> brs = filesReaderedBuffer.get(topicName);

        brs.forEach((BufferedReader br) -> {
            try {
                String lineContent = br.readLine();
                while (lineContent != null) {
//                    try {
                    result.add(baseDec.decode(lineContent.getBytes()));
//                    } catch (Exception e) {
//                        System.out.println(lineContent);
//                    }
                    lineContent = br.readLine();
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        });
        return result;
    }

}
