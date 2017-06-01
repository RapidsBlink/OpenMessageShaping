package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.ArrayList;
import java.util.Collection;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private ArrayList<String> nameList;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {

        return null;
    }

    @Override
    public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void attachQueue(String queueName, Collection<String> topics) {
        nameList.addAll(topics);
        nameList.add(queueName);
    }

    private boolean isNameInMyNameList(String name) {
        for (String nameString : nameList) {
            if (nameString.equals(name))
                return true;
        }
        return false;
    }
}
