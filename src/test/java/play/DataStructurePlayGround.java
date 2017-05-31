package play;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by yche on 5/25/17.
 */
public class DataStructurePlayGround {
    private static void testHashMap() {
        HashMap<String, String> myMap = new HashMap<>();
        myMap.put("key1", "val");
        myMap.forEach((k, v) -> {
            System.out.println(k + v);
        });
        myMap.put("key1", "val2");
        myMap.forEach((k, v) -> {
            System.out.println(k + v);
        });

    }

    private static void testArrayList() {
        ArrayList<HashSet<Integer>> myArray = new ArrayList<>(10);
        System.out.println(myArray.size());
        myArray.forEach((hashSet) -> {
            if (hashSet == null)
                System.out.println("null");
            else
                System.out.println(hashSet.size());
        });
    }

    private static void testArrayCreationWithDefaultConstructing() {
        Integer MAX_NAME_NUM = 128;
        Integer[] data = new Integer[60];
        // should not use it, refers to the same object...
        Arrays.fill(data, 0);
        List<Integer> list = Arrays.asList(data);
        list.forEach((integer) -> {
            System.out.print(integer);
        });

        ReentrantLock[] updateHeaderLineLockArr = new ReentrantLock[MAX_NAME_NUM];
        // correct initialization
        for (int i = 0; i < updateHeaderLineLockArr.length; i++) {
            updateHeaderLineLockArr[i] = new ReentrantLock();
        }
        for (int i = 0; i < updateHeaderLineLockArr.length; i++) {
            System.out.println(i + "," + updateHeaderLineLockArr[i] + updateHeaderLineLockArr[0].equals(updateHeaderLineLockArr[i]));
        }

        ArrayList<Integer>[] myInt = new ArrayList[MAX_NAME_NUM];
        for (int i = 1; i < myInt.length; i++) {
            myInt[i] = new ArrayList<>();
            myInt[i].add(i);
        }
        for (int i = 1; i < myInt.length; i++) {
            System.out.println(i + "," + myInt[i] + myInt[i].equals(myInt[0]));
        }
    }


}
