

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Stream;

public class MyKafkaConsumer extends KafkaConsumer {

    private static Logger LOGGER = LoggerFactory.getLogger(MyKafkaConsumer.class);

    public MyKafkaConsumer(Map configs) {
        super(configs);
    }

    @Override
    public ConsumerRecords poll(long timeoutMs) {
        try (Stream<String> stream = Files.lines(Paths.get("/tmp/mount/shutdown.conf"))) {
            stream.forEach(line -> {
                if(line.equalsIgnoreCase("true")){
                    this.pause(this.assignment());
                    LOGGER.info("Kafka Consumer is paused");
                }else if(line.equalsIgnoreCase("false")){
                    this.resume(this.assignment());
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        return super.poll(timeoutMs);
    }
}
