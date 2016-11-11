package com.logitravel.kafka.mirror;

import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Properties;
import java.util.UUID;

/**
 * KafkaSpout.
 */
public class KafkaSpout extends org.apache.storm.kafka.KafkaSpout {

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

    // Get scheme classname from properties or use StringScheme by default
    String schemeClassname = (String) props.getOrDefault("spout.scheme",
                                                         "org.apache.storm.kafka.StringScheme");

    Class<?> clazz = Class.forName(schemeClassname);
    conf.scheme = new SchemeAsMultiScheme((Scheme) clazz.newInstance());
    return conf;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
  }

}
