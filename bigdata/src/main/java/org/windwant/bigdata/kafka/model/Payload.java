package org.windwant.bigdata.kafka.model;

import java.io.Serializable;

/**
 * 消息体对象
 * Created by Administrator on 18-7-31.
 */
public class Payload implements Serializable{

    private static final long serialVersionUID = 8851810168028246531L;
    private int status;

    private String cause;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    @Override
    public String toString() {
        return "Payload{" +
                "status=" + status +
                ", cause='" + cause + '\'' +
                '}';
    }
}
