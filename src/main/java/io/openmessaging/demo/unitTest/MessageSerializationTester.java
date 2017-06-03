package io.openmessaging.demo.unitTest;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.MessageDeserialization;
import io.openmessaging.demo.MessageSerialization;

import java.util.logging.Logger;

/**
 * Created by will on 27/5/2017.
 */
public class MessageSerializationTester {
    private static Logger LOGGER = Logger.getLogger("InfoLogging");

    private static void test0() {
        MessageSerialization msgSer = new MessageSerialization();
        MessageDeserialization msgDser = new MessageDeserialization();

        boolean OK = true;
        for (int i = 0; i < 10; i++) {
            Message message = new DefaultBytesMessage(("123456_" + i).getBytes());
            message.putProperties("p", "asa");
            message.putProperties("b" + i, i);
            message.putHeaders(MessageHeader.TOPIC, "TOPIC" + i);
            message.putHeaders("h", 111L);

            byte[] binaryData = msgSer.serialize((DefaultBytesMessage) message);
            DefaultBytesMessage ret = msgDser.deserialize(binaryData);
            byte[] binaryData2 = msgSer.serialize(ret);
            LOGGER.info("TOPIC: " + ret.headers().getString(MessageHeader.TOPIC));

            for (int j = 0; j < binaryData.length; j++) {
                if (binaryData[j] != binaryData2[j]) {
                    LOGGER.warning("ERROR!");
                    OK = false;
                    break;
                }
            }

            if (OK) {
                LOGGER.info("Correct!");
            }
        }
    }

    public static void main(String[] args) {
        test0();
    }

}