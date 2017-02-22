package com.logitravel.kafka.mirror;

import backtype.storm.utils.Time;
import org.junit.Test;

/**
 * Test topology.
 */
public class TopologyTest {

  @Test
  public void main() throws Exception {

    // Argument list to test
    String consumerConf = "--consumer.config consumer.properties";
    String producerConf = "--producer.config producer.properties";
    String topic = "-t AGC.Flat";

    // Run main
    Topology.main(String.format("%s %s %s", consumerConf, producerConf, topic).split(" "));

    // Wait 10 seconds and exit
    Time.sleep(30 * 1000);
  }

}