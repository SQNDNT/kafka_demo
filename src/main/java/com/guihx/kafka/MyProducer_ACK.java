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

        /**
         * 配置ack，仅同步下有效
         * 0:消息发送后不用等broker返回ack，高性能低安全，易发生消息丢失
         * 1:默认配置，消息发送后需要leader将消息写到本地log中后再返回ack,是个均衡模式
         * -1/all:消息发送后leader接收并同步给follow后返回ack，低性能高安全。须配置min.insync.replicas=2（默认是1，最好配置>=2，否则效果跟ack=1一样）
         * @Author guihx
         * @Date 2021-11-1 14:26
         */
        props.put(ProducerConfig.ACKS_CONFIG, 1);
        //发送消息失败后重试3次
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //重发送间隔(300毫秒)
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        //kafka消息缓冲区32m（默认）
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32*1024*1024);
        //一次拉取16kb消息（默认）
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16*1024);
        //拉不满16的话，10毫秒后也发送
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);


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
