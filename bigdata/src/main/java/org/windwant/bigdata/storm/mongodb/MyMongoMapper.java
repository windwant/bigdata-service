package org.windwant.bigdata.storm.mongodb;

import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.tuple.ITuple;
import org.bson.Document;

/**
 * Created by Administrator on 18-9-17.
 */
public class MyMongoMapper implements MongoMapper {
    private String[] fields;
    @Override
    public Document toDocument(ITuple iTuple) {
        Document document = new Document();
        for (String field : fields) {
            document.append(field, iTuple.getValueByField(field));
        }
        return document;
    }

    public MyMongoMapper withFields(String... fields){
        this.fields = fields;
        return this;
    }
}
