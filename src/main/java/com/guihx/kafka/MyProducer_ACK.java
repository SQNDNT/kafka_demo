package com.guihx.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyProducer_ACK {

    private final static String TOPIC_NAME = "test";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //键值对序列化
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC_NAME, JSON.toJSONString("同步消息"));

        //发送端同步消息（要等kafka的ask）
        RecordMetadata metadata = null;
        try {
            metadata = producer.send(producerRecord).get();
            System.out.println("同步方式发送消息结果: topic-"+ metadata.topic()+" | Partition-"+metadata.partition()+" | offset-"+ metadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


        producer.close();
    }
}
