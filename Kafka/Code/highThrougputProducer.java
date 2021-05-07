import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerWithKeys {

    public static void main (String [] args){

        Logger logger = LoggerFactory.getLogger(MyProducer1.class);
        //property file
        String bootstrapServer="localhost:9092";
        Properties property=new Properties();
        property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        property.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        property.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        
        //set safe producer property
        property.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        property.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        property.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        property.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); //kafka  2.0 >1.1 else 1 otherwise

        // set for high throughput producer by introducing some delay

        property.setProperty(ProducerConfig.LINGER_MS_CONFIG,"5");
        property.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        property.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

        //create producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(property);

        for (int i =0; i <=10 ; i++){
            String key="ID_"+Integer.toString(i);
            String topic="my_first_topic";
            String value="Kafka Learning"+Integer.toString(i);
            ProducerRecord<String,String> record= new ProducerRecord<>(topic,key,value);

            //send record
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e==null){

                        logger.info("RecordMetadata is returned" +"\n"+
                                "Topic: "+recordMetadata.topic() +"\n"+
                                "Partition: "+recordMetadata.partition() +"\n"+
                                "Offset: "+recordMetadata.offset() +"\n"+
                                "timestamp: "+ recordMetadata.timestamp());
                    }
                    else{
                        logger.error("Error while producing",e);
                    }
                }
            });

        }
        producer.flush();
        producer.close();

    }
}
