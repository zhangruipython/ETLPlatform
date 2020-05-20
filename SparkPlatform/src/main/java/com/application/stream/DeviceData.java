package com.application.stream;

/**
 * @author 张睿
 * @create 2020-05-20 13:21
 * streaming 接受kafka数据格式
 **/
public class DeviceData {
    private String value;
    private String key;
    private String topic;
    private Integer partition;
    private Long offset;
    private Long timestamp;
    private Integer timestampType;

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getTimestampType() {
        return timestampType;
    }

    public void setTimestampType(Integer timestampType) {
        this.timestampType = timestampType;
    }
}

