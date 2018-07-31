package org.windwant.bigdata.kafka.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.*;
import java.util.Map;

/**
 * 对象反序列化
 * Created by Administrator on 18-7-31.
 */
public class EntityDeserializer implements Deserializer {

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        try {
            ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
            ObjectInputStream in = new ObjectInputStream(bin);
            return in.readObject();
        } catch (IOException e) {
            throw new SerializationException("IO Error when deserializing Object from byte[] ");
        } catch (ClassNotFoundException e) {
            throw new SerializationException(e.getCause());
        }
    }

    @Override
    public void close() {

    }
}
