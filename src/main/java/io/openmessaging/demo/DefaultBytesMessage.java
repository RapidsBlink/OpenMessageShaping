package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {
    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties = new DefaultKeyValue();
    private String bodyString;

    public DefaultBytesMessage(byte[] body) {
        this.bodyString = new String(body);
    }

    @Override
    public byte[] getBody() {
        return bodyString.getBytes();
    }

    @Override
    public BytesMessage setBody(byte[] body) {
        this.bodyString = new String(body);
        return this;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        properties.put(key, value);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(bodyString).append(Constants.OBJ_SPLITTER);
        ((DefaultKeyValue) headers).kvs.forEach((key, value) -> {
            sb.append(key).append(Constants.PAIR_SPLITTER).append(value);
            sb.append(Constants.ITEM_SPLITTER);
        });

        sb.append(Constants.OBJ_SPLITTER);
        ((DefaultKeyValue) properties).kvs.forEach((key, value) -> {
            sb.append(key).append(Constants.PAIR_SPLITTER).append(value);
            sb.append(Constants.ITEM_SPLITTER);
        });

        if (properties.keySet().size() > 0) {
            sb.setLength(sb.length() - 1);
        }
        sb.append(Constants.OBJ_SPLITTER);
        return sb.toString();
    }

    static public DefaultBytesMessage valueOf(String myString) {
//        System.out.println(myString);
        String[] threeObjects = myString.split(Constants.OBJ_SPLITTER);
        DefaultBytesMessage message = new DefaultBytesMessage(threeObjects[0].getBytes());
        if (threeObjects[1].length() > 0) {
            for (String item : threeObjects[1].split(Constants.ITEM_SPLITTER)) {
                String[] pair = item.split(Constants.PAIR_SPLITTER);
                message.putHeaders(pair[0], pair[1]);
            }
        }
        if (threeObjects.length >= 3) {
            if (threeObjects[2].length() > 0) {
                for (String item : threeObjects[2].split(Constants.ITEM_SPLITTER)) {
                    String[] pair = item.split(Constants.PAIR_SPLITTER);
                    message.putProperties(pair[0], pair[1]);
                }
            }
        }
        return message;
    }
}