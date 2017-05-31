package play;

/**
 * Created by yche on 5/26/17.
 */
public class VisibilityTest {
    public static void main(String[] args) {
        IndexInfo myIndexInfo = new IndexInfo(1L, 2);
        System.out.println(myIndexInfo.offset + "," + myIndexInfo.length);
    }
}
