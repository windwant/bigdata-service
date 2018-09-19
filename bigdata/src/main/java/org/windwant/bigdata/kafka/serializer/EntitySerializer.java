package org.windwant.bigdata.kafka.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

/**
 * 对象序列化
 * 发送对象实体消息 序列化使用
 * Created by Administrator on 18-7-31.
 */
public class EntitySerializer implements Serializer {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Object data) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try(ObjectOutputStream out = new ObjectOutputStream(bout)) {//资源释放
            out.writeObject(data);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("IO Error when serializing Object to byte[] ");
        }
    }

    @Override
    public void close() {

    }
}
