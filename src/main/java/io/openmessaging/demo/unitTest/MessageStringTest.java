package io.openmessaging.demo.unitTest;

import io.openmessaging.demo.DefaultBytesMessage;

/**
 * Created by yche on 6/4/17.
 */
public class MessageStringTest {
    public static void main(String[] args) {
        DefaultBytesMessage defaultBytesMessage = new DefaultBytesMessage(new String("Hello").getBytes());
        defaultBytesMessage.putHeaders("h1", "pro1_val");
        defaultBytesMessage.putHeaders("h2", 1);
        defaultBytesMessage.putHeaders("head2", 1.0);
        defaultBytesMessage.putProperties("pro2", 1);
        defaultBytesMessage.putProperties("pro3", 2L);

        System.out.println(defaultBytesMessage.toString());
        DefaultBytesMessage another = DefaultBytesMessage.valueOf(defaultBytesMessage.toString());
        System.out.println(another.toString());
    }
}
