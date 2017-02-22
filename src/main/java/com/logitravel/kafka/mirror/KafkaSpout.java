package com.logitravel.kafka.mirror;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Properties;
import java.util.UUID;

/**
 * KafkaSpout.
 */
public class KafkaSpout extends storm.kafka.KafkaSpout {

  /**
   * Creates a Kafka spout.
   */
  public KafkaSpout(Properties properties, String topic)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    super(extractSpoutConfig(properties, topic));
  }

   /**
   * Extract spout config from Topology configuration.
   * @param props    The topology configuration.
   * @return         An SpoutConfig object
   */
  private static SpoutConfig extractSpoutConfig(Properties props, String topic)
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    // Zookeeper hosts
    ZkHosts zkHosts = new ZkHosts(props.getProperty("zookeeper.connect"));

    // Client ID
    String clientId = props.getProperty("consumer.id");
    clientId = clientId.length() > 0 ?  clientId : UUID.randomUUID().toString();

    // Spout config
    SpoutConfig conf = new SpoutConfig(zkHosts, topic, "/kafka-mirror/" + topic, clientId);
    return conf;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
  }

}
