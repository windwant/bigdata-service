package org.windwant.bigdata.kafka.serializer;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.*;
import java.util.Map;

/**
 * 对象反序列化
 * 接收实体类消息，反序列化
 * Created by Administrator on 18-7-31.
 */
public class EntityDeserializer<T> implements Deserializer {

    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tType;

    @Override
    public void configure(Map map, boolean b) {
        if(map.containsKey("value.type.class")){
            tType = (Class<T>) map.get("value.type.class");
        }
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }
        if(tType == null){
            throw new SerializationException("please check the config props: value.type.class");
        }
//        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
//        try ( ObjectInputStream in = new ObjectInputStream(bin)){
//            return in.readObject();
//        } catch (IOException e) {
//            throw new SerializationException("IO Error when deserializing Object from byte[] ");
//        } catch (ClassNotFoundException e) {
//            throw new SerializationException(e.getCause());
//        }

        try {
            return objectMapper.readValue(bytes, tType);
        } catch (IOException e) {
            e.printStackTrace();
            throw new SerializationException(e.getCause());
        }
    }

    @Override
    public void close() {

    }
}
