package com.logitravel.kafka.mirror;

import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

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
  public KafkaProducer(String topic) {
    this();
    withTopicSelector(new DefaultTopicSelector(topic));
    withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "message"));
  }

}
