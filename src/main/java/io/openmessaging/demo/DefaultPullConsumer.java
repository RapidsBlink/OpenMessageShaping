package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.util.ArrayList;
import java.util.Collection;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private ArrayList<String> myNameList = new ArrayList<>();
    private int localTopicIndex = 0;
    private int innerTopicIndex = 0;

    private ArrayList<DefaultBytesMessage> currentTopic = null;

    private DataReader dr;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        dr = new DataReader(properties.getString("STORE_PATH"));
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message poll() {
        //System.out.println("Consumer poll.");
        if (currentTopic == null || innerTopicIndex == currentTopic.size()) {
            if (localTopicIndex >= myNameList.size()) {
                return null;
            }
            if(currentTopic != null){
                dr.finishedTopic(myNameList.get(localTopicIndex - 1));
            }
            fetchNextTopicMessageList();
        }
        innerTopicIndex++;
        return currentTopic.get(innerTopicIndex - 1);
    }

    private void fetchNextTopicMessageList() {
        innerTopicIndex = 0;
        currentTopic = dr.getTopicArrayList(myNameList.get(localTopicIndex));
        localTopicIndex++;

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
        ArrayList<String> nameList = new ArrayList<>();
        nameList.addAll(topics);
        nameList.add(queueName);
        ArrayList<String> dataFileOrderedTopics = dr.topicsReverseOrderInDataFile();
        for (String topic : dataFileOrderedTopics) {
            for (int i = 0; i < nameList.size(); i++) {
                if (nameList.get(i).equals(topic))
                    myNameList.add(topic);
            }
        }
        dr.countTopicListenerNumber(myNameList);
    }

}
