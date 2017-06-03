package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.openmessaging.demo.Constants.MAX_MESSAGE_SIZE;

public class DefaultProducer implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private KeyValue properties;

    private MessageSerialization messageSerialization = new MessageSerialization();
    private ByteBuffer byteBuffer = ByteBuffer.allocate(MAX_MESSAGE_SIZE);
    private DataDumper dataDumper = null;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        try {
            this.dataDumper = new DataDumper(properties.getString("STORE_PATH"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override
    public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public void send(Message message) {
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);

        byteBuffer.clear();
        messageSerialization.serialize((DefaultBytesMessage) message, byteBuffer);
        byteBuffer.flip();
        try {
            dataDumper.writeToFile(topic != null ? topic : queue, byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override
    public void flush() {
        try {
            dataDumper.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
