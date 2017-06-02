package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

import java.util.ArrayList;
import java.util.Collection;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private ArrayList<String> myNameList = new ArrayList<>();

    private DataReader dataReader;
    private ArrayList<DefaultBytesMessage> messageList = new ArrayList<>();
    private boolean isFirstTime = true;
    private int messageIdx = 0;
    private boolean isEnd = false;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        dataReader = new DataReader(properties.getString("STORE_PATH"), this);
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        if (isFirstTime) {
            updateInternalStates();
        }

        if (hasNext()) {
            return next();
        } else {
            return null;
        }
    }

    private void waitForNextMessageList() {
        messageList = dataReader.requestNextChunkMessage(this);
    }

    private boolean hasNext() {
        return !isEnd;
    }

    private void updateInternalStates() {
        // already loop over one bulk-sync-message-list, wait for the next one
        while (messageIdx >= messageList.size()) {
            waitForNextMessageList();
            messageIdx = 0;
            if (dataReader.isEnd()) {
                isEnd = true;
                return;
            } else {
                // find next valid message
                while (messageIdx < messageList.size() && !isMessageInMyNameList(messageList.get(messageIdx))) {
                    messageIdx++;
                }
            }
        }
    }

    private Message next() {
        Message msg = messageList.get(messageIdx);
        updateInternalStates();
        return msg;
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
        myNameList.addAll(topics);
        myNameList.add(queueName);
    }

    private boolean isMessageInMyNameList(Message message) {
        String name = message.headers().getString(MessageHeader.TOPIC);
        if (name == null)
            name = message.headers().getString(MessageHeader.QUEUE);
        for (String myName : myNameList) {
            if (myName.equals(name))
                return true;
        }
        return false;
    }
}
