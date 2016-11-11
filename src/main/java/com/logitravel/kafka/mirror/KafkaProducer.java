package com.logitravel.kafka.mirror;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Properties;

/**
 * KafkaProducer.
 */
public class KafkaProducer extends KafkaBolt {

  public KafkaProducer() {
    super();
  }

  /**
   * Constructor.
   * @param topic The topic to produce to.
   */
  @SuppressWarnings("unchecked")
  public KafkaProducer(String topic, Properties properties) {
    this();
    withTopicSelector(new DefaultTopicSelector(topic));
    withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "message"));
    withProducerProperties(properties);
  }

}
