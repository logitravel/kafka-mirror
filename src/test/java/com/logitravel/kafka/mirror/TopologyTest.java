package com.logitravel.kafka.mirror;

import org.apache.storm.utils.Time;
import org.junit.Test;

/**
 * Test topology.
 */
public class TopologyTest {

  @Test
  public void main() throws Exception {

    // Argument list to test
    String consumerConf = "--consumer.config test_consumer.properties";
    String producerConf = "--producer.config test_producer.properties";
    String topic = "-t test";

    // Run main
    Topology.main(String.format("%s %s %s", consumerConf, producerConf, topic).split(" "));

    // Wait 10 seconds and exit
    Time.sleep(10 * 1000);
  }

}