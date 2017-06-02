import io.openmessaging.tester.ConsumerTester;
import io.openmessaging.tester.ProducerTester;

public class ProducerAndConsumerTest {


    public static void main(String[] args) throws Exception {
        //new io.openmessaging.demo.unitTest.MessageDumpTester().multiThreadsWriter();
        ProducerTester.main(null);
        ConsumerTester.main(null);
        //new io.openmessaging.demo.unitTest.MessageDumpTester().multiThreadsReader();
    }
}
