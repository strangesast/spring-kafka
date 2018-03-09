package deadsimple.producer;

import deadsimple.generated.ApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

public class Sender {
  @Value("${kafka.topic.avro}")
  private String avroTopic;

  @Autowired
  private KafkaTemplate<String, ApplicationState> kafkaTemplate;

  public void send(ApplicationState applicationState) {
    kafkaTemplate.send(avroTopic, applicationState);
  }
}
