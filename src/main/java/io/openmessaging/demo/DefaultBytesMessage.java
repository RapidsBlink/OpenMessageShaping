package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

public class DefaultBytesMessage implements BytesMessage {
    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties = null;
    private String bodyString;

    public DefaultBytesMessage(byte[] body) {
        this.bodyString = new String(body);
    }

    public DefaultBytesMessage(String body) {
        this.bodyString = body;
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
        if (properties == null)
            properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        if (properties == null)
            properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        if (properties == null)
            properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        if (properties == null)
            properties = new DefaultKeyValue();
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

        if (properties != null) {
            ((DefaultKeyValue) properties).kvs.forEach((key, value) -> {
                sb.append(key).append(Constants.PAIR_SPLITTER).append(value);
                sb.append(Constants.ITEM_SPLITTER);
            });
        }
        sb.append(Constants.OBJ_SPLITTER);

        return sb.toString();
    }

    static public DefaultBytesMessage valueOf(String myString) {
        StringBuilder bodyStr = new StringBuilder();
        StringBuilder keyStr = new StringBuilder();
        StringBuilder valueStr = new StringBuilder();

        // 1st: read body
        int currIndex = 0;
        char ch;
        for (; (ch = myString.charAt(currIndex)) != Constants.OBJ_SPLITTER; currIndex++) {
            bodyStr.append(ch);
        }
        DefaultBytesMessage message = new DefaultBytesMessage(bodyStr.toString());
        currIndex++;

        // 2nd: read headers
        while (myString.charAt(currIndex) != Constants.OBJ_SPLITTER) {
            keyStr.setLength(0);
            for (; (ch = myString.charAt(currIndex)) != Constants.PAIR_SPLITTER; currIndex++) {
                keyStr.append(ch);
            }
            currIndex++;

            valueStr.setLength(0);
            for (; (ch = myString.charAt(currIndex)) != Constants.ITEM_SPLITTER; currIndex++) {
                valueStr.append(ch);
            }
            currIndex++;
            message.putHeaders(keyStr.toString(), valueStr.toString());
        }
        currIndex++;

        // 3rd: read properties
        while (myString.charAt(currIndex) != Constants.OBJ_SPLITTER) {
            keyStr.setLength(0);
            for (; (ch = myString.charAt(currIndex)) != Constants.PAIR_SPLITTER; currIndex++) {
                keyStr.append(ch);
            }
            currIndex++;

            valueStr.setLength(0);
            for (; (ch = myString.charAt(currIndex)) != Constants.ITEM_SPLITTER; currIndex++) {
                valueStr.append(ch);
            }
            message.putProperties(keyStr.toString(), valueStr.toString());
        }

        return message;
    }
}