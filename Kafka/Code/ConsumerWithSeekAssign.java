import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerWithSeekAssign {

    public static void main(String [] args){

        //Create logger
        Logger logger = LoggerFactory.getLogger(Consumer.class);
        String bootstrapServer="localhost:9092";
        String topic="my_first_topic";
        String groupID="first_consumer_group";
        //Consumer property
        Properties properties= new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //Create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<>(properties);

       // Assign and seek to replay data or fetch a specific message
        TopicPartition partitiontoReadFrom = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitiontoReadFrom));

        //seek
        long offsetToReadFrom=40L;
        consumer.seek(partitiontoReadFrom,offsetToReadFrom);

        int numberOfMessageToRead=5;
        int numberOfMessageRead=0;
        boolean flag=true;

        //poll for messages
        while (flag){
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String,String> record : records){
                numberOfMessageRead+=1;
                logger.info("key: " +record.key() + " Value: "+ record.value());
                logger.info("offset: "+ record.offset() + "Partition: "+ record.partition());
                if(numberOfMessageRead >= numberOfMessageToRead){
                    flag=false;
                    break;
                }
            }

        }

    }
}
