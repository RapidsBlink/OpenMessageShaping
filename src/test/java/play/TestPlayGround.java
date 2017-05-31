package play;

/**
 * Created by yche on 5/28/17.
 */
public class TestPlayGround {
    public static void main(String[] args) {
        IndexInfo myIndexInfo = new IndexInfo(1L, 2);
        System.out.println(myIndexInfo.offset + "," + myIndexInfo.length);
    }
}
