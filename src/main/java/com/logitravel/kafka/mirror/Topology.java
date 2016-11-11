package com.logitravel.kafka.mirror;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Topology definition.
 */
public class Topology {

  private static final Integer DEFAULT_WORKERS = 3;
  private static final Integer DEFAULT_EXECUTORS = 3;

  /**
   * Main program.
   * @param args Argument string array
   */
  public static void main(String[] args) {

    // Parser
    CommandLineParser parser = new DefaultParser();

    // Options
    Options options = new Options();

    // Consumer (spout)
    options.addOption(
        Option.builder("c")
              .hasArg(true)
              .required(false)
              .argName("consumer.config")
              .longOpt("consumer.config")
              .desc("Consumer config file")
              .build());

    // Producer (bolt)
    options.addOption(
        Option.builder("p")
              .hasArg(true)
              .required(false)
              .argName("producer.config")
              .longOpt("producer.config")
              .desc("Producer config file")
              .build());

    // Topic list
    options.addOption(
        Option.builder("t")
              .hasArg(true)
              .required(true)
              .argName("topic")
              .longOpt("topic")
              .desc("Topic(s). Comma separated topic1,topic2, ...")
              .build());

    // Topology name
    options.addOption(
        Option.builder("n")
              .hasArg(true)
              .required(false)
              .argName("name")
              .longOpt("name")
              .desc("Topology name, required for deployment")
              .build());

    // Workers
    options.addOption(
        Option.builder("w")
              .hasArg(true)
              .required(false)
              .argName("workers")
              .longOpt("workers")
              .desc(String.format("Workers (default: %s)", DEFAULT_WORKERS))
              .build());

    // Executors
    options.addOption(
        Option.builder("e")
              .hasArg(true)
              .required(false)
              .argName("executors")
              .longOpt("executors")
              .desc(String.format("Executors (default: %s)", DEFAULT_EXECUTORS))
              .build());

    try {

      // Command line
      CommandLine line = parser.parse(options, args);

      final String workers = line.getOptionValue("workers", String.valueOf(DEFAULT_WORKERS));
      final String executors = line.getOptionValue("executors", String.valueOf(DEFAULT_EXECUTORS));

      // Topology config
      Config conf = new Config();
      conf.setNumAckers(Integer.valueOf(workers));
      conf.setNumWorkers(Integer.valueOf(workers));

      // Topology builder
      TopologyBuilder builder = new TopologyBuilder();

      // Kafka write hosts
      final Properties consumerProperties = new Properties();
      try {
        String consumerConfigFile = "etc/consumer.properties";
        if (line.hasOption("consumer.config")) {
          consumerConfigFile = line.getOptionValue("consumer.config");
        }
        InputStream consumerPropsIo = new FileInputStream(consumerConfigFile);
        consumerProperties.load(consumerPropsIo);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        System.exit(127);
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(127);
      }

      final Properties producerProperties = new Properties();
      try {
        String producerConfigFile = "etc/producer.properties";
        if (line.hasOption("producer.config")) {
          producerConfigFile = line.getOptionValue("producer.config");
        }
        InputStream producerPropsIo = new FileInputStream(producerConfigFile);
        producerProperties.load(producerPropsIo);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        System.exit(127);
      } catch (IOException e) {
        e.printStackTrace();
        System.exit(127);
      }

      // Split topics and add a Spout-Producer pair
      final List<String> topics = Arrays.asList(line.getOptionValue("topic").split(","));
      for (String topic: topics) {

        final String consumerName = String.format("%s-CONSUMER", topic);
        final String producerName = String.format("%s-PRODUCER", topic);

        // Setup spout
        builder.setSpout(consumerName,
                         new KafkaSpout(consumerProperties, topic),
                         Integer.valueOf(executors));

        // Setup bolt
        builder.setBolt(producerName,
                        new KafkaProducer(topic, producerProperties),
                        Integer.valueOf(executors))
               .shuffleGrouping(consumerName);
      }

      // Live deployment
      if (line.hasOption("name")) {
        StormSubmitter.submitTopology(line.getOptionValue("name"),
                                      conf,
                                      builder.createTopology());

      } else {

        // Local deployment
        LocalCluster cluster;
        cluster = new LocalCluster();
        cluster.submitTopology("kafka-mirror-local-topology", conf, builder.createTopology());
      }

    } catch (MissingOptionException e) {
      System.err.println(e.getMessage());
      printHelp(options, 127);
    } catch (ParseException e) {
      e.printStackTrace();
    } catch (InvalidTopologyException e) {
      e.printStackTrace();
    } catch (AlreadyAliveException e) {
      e.printStackTrace();
    } catch (AuthorizationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

  }

  /**
   * Print help.
   * @param exitCode Exit code
   */
  private static void printHelp(Options options, Integer exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.setWidth(130);
    formatter.printHelp(Topology.class.getName(), options);
    System.exit(exitCode);
  }

}
