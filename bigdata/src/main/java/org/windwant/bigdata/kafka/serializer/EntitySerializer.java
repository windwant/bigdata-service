package org.windwant.bigdata.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.windwant.bigdata.kafka.model.Payload;

import java.io.*;
import java.util.Map;

/**
 * 对象序列化
 * 发送对象实体消息 序列化使用
 * Created by Administrator on 18-7-31.
 */
public class EntitySerializer<T> implements Serializer {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tType;

    @Override
    public void configure(Map map, boolean b) {
        if(map.containsKey("value.type.class")){
            tType = (Class<T>) map.get("value.type.class");
        }
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if(data == null){
            return null;
        }
        if(tType == null){
            throw new SerializationException("please check the config props: value.type.class");
        }

//        ByteArrayOutputStream bout = new ByteArrayOutputStream();
//        try(ObjectOutputStream out = new ObjectOutputStream(bout)) {//资源释放
//            out.writeObject(data);
//            return bout.toByteArray();
//        } catch (IOException e) {
//            throw new SerializationException("IO Error when serializing Object to byte[] ");
//        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new SerializationException(e.getCause());
        }
    }

    @Override
    public void close() {

    }
}
