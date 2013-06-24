/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.common.logging;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Logging configuration helper.
 */
public final class LoggingConfiguration {
  // Common between Distributed and Single Node
  public static final String LOG_PATTERN = "log.pattern";
  public static final String LOG_BASE_DIR = "log.base.dir";
  public static final String LOG_FILE_SYNC_INTERVAL_BYTES = "log.file.sync.interval.bytes";

  // Used only in Distributed mode
  public static final String NUM_PARTITIONS = "log.publish.num.partitions";
  public static final String KAFKA_SEED_BROKERS = "kafka.seed.brokers";
  public static final String LOG_SAVER_RUN_ACCOUNT = "log.saver.run.account";
  public static final String LOG_SAVER_RUN_MEMORY_MB = "log.saver.run.memory.megs";
  public static final String LOG_RETENTION_DURATION_MS = "log.retention.duration.ms";
  public static final String LOG_MAX_FILE_SIZE_BYTES = "log.max.file.size.bytes";

  // Used only in Single Node mode
  public static final String LOG_FILE_ROTATION_INTERVAL_MINS = "log.file.rotation.interval.mins";

  private LoggingConfiguration() {}

  /**
   * Given a string of format "host1:port1,host2:port2", the function returns a list of Kafka hosts.
   * @param seedBrokers String to parse the host/port list from.
   * @return list of Kafka hosts.
   */
  public static List<KafkaHost> getKafkaSeedBrokers(String seedBrokers) {
    List<KafkaHost> kafkaHosts = Lists.newArrayList();
    for (String hostPort : Splitter.on(",").trimResults().split(seedBrokers)) {
      Iterable<String> hostPortList = Splitter.on(":").trimResults().split(hostPort);

      Iterator<String> it = hostPortList.iterator();
      kafkaHosts.add(new KafkaHost(it.next(), Integer.parseInt(it.next())));
    }
    return kafkaHosts;
  }

  /**
   * Represents a Kafka host with hostname and port.
   */
  public static class KafkaHost {
    private final String hostname;
    private final int port;

    public KafkaHost(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }
  }
}
