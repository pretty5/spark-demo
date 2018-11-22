import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;

public class Tets {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        String topic = "lol";
        int partition =0;
        long offset = 0;
        String consumerGroup = "f";
        //定义标志  是否是第一次消费
        boolean first =true;
        Properties properties = new Properties();
        properties.put("bootstrap.servers","localhost:9092");
        properties.put("group.id",consumerGroup);
        properties.put("enable.auto.commit","false");
        properties.put("auto.commit.interval.ms","1000");
        properties.put("auto.offset.reset","earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456");
        connection.setAutoCommit(false);
        PreparedStatement select = connection.prepareStatement("select * from kafka_consumer where consumer_group=?");
        select.setString(1,consumerGroup);
        ResultSet resultSet = select.executeQuery();
        HashMap<TopicPartition, Long> partitionOffset = new HashMap<TopicPartition, Long>();

        while (resultSet.next()){
            first=false;
            topic = resultSet.getString(1);
            partition =resultSet.getInt(2);
            offset = resultSet.getLong(3);
            consumerGroup = resultSet.getString(4);
            TopicPartition tp = new TopicPartition(topic, partition);
            partitionOffset.put(tp,offset);
        }
        if (first){
            kafkaConsumer.subscribe(Arrays.asList(topic));
        }else {
            Set<TopicPartition> topicPartitions = partitionOffset.keySet();
            kafkaConsumer.assign(topicPartitions);
            for (TopicPartition tp: topicPartitions){
                kafkaConsumer.seek(tp,partitionOffset.get(tp)+1);
            }
        }
        while (true){
            try {
                Thread.sleep(5000);
                PreparedStatement update = connection.prepareStatement("");
                ConsumerRecords<String,String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record:records){
                    System.out.printf("offset=%d,value=%s",record.offset(),record.value());
                }
            } catch (Exception e) {
                System.out.println(e);
                connection.rollback();
            }finally {

            }
        }

    }
}
