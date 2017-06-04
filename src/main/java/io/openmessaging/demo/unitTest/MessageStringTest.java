package io.openmessaging.demo.unitTest;

import io.openmessaging.demo.DefaultBytesMessage;

/**
 * Created by yche on 6/4/17.
 */
public class MessageStringTest {
    public static void main(String[] args) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(new String("Hello").getBytes());
        defaultBytesMessage.putHeaders("pro1", "pro1_val");
        defaultBytesMessage.putHeaders("pro2", 1);
        defaultBytesMessage.putHeaders("pro3", 2L);

        System.out.println(defaultBytesMessage.toString());
        DefaultBytesMessage another = DefaultBytesMessage.valueOf(defaultBytesMessage.toString());
        System.out.println(another.toString());
    }
}
