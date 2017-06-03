package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.Collection;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private NaiveDataReader naiveDataReader;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        this.naiveDataReader = new NaiveDataReader(properties.getString("STORE_PATH"));
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (naiveDataReader.hasNext()) {
            return naiveDataReader.next();
        } else {
            return null;
        }
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
        naiveDataReader.attachNames(queueName, topics);
    }
}
