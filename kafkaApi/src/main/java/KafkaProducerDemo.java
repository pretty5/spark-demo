/*
*@ClassName:KafkaProducerDemo
 @Description:TODO
 @Author:
 @Date:2018/11/20 9:28 
 @Version:v1.0
*/
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class KafkaProducerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");

        properties.put("enable.idempotence",true);

        properties.put("retries", 3);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);

            for (int i = 0; i < 100; i++) {
                String msg = "Message " + i;
                producer.send(new ProducerRecord<String, String>("lol", msg));
                System.out.println("Sent:" + msg);
            }
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }
    }

}
