package ru.hh.rabbitmq.spring;

import static ru.hh.rabbitmq.spring.ConfigKeys.CHANNEL_CACHE_SIZE;
import static ru.hh.rabbitmq.spring.ConfigKeys.CLOSE_TIMEOUT;
import static ru.hh.rabbitmq.spring.ConfigKeys.CONNECTION_TIMEOUT_MS;
import static ru.hh.rabbitmq.spring.ConfigKeys.HEARTBIT_SEC;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOST;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_PORT_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.HOSTS_SEPARATOR;
import static ru.hh.rabbitmq.spring.ConfigKeys.PASSWORD;
import static ru.hh.rabbitmq.spring.ConfigKeys.PORT;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_CONFIRMS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.PUBLISHER_RETURNS;
import static ru.hh.rabbitmq.spring.ConfigKeys.RECEIVER_HOSTS;
import static ru.hh.rabbitmq.spring.ConfigKeys.USERNAME;
import static ru.hh.rabbitmq.spring.ConfigKeys.VIRTUALHOST;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import ru.hh.rabbitmq.spring.send.PublisherBuilder;

/**
 * <p>
 * Create and configure {@link Receiver} and/or {@link PublisherBuilder}.
 * </p>
 * <p>
 * See {@link ConfigKeys} constants for configuration options.
 * </p>
 */
public class ClientFactory {

  private PropertiesHelper properties;
  private List<ConnectionFactory> factories;

  public ClientFactory(Properties properties) {
    this.properties = new PropertiesHelper(properties);
  }

  /**
   * Create new receiver. Reuse connections to brokers. Won't use {@link ConfigKeys#RECEIVER_HOSTS}.
   * 
   * @return new receiver
   */
  public Receiver createReceiver() {
    return createReceiver(true);
  }

  /**
   * Create new receiver.
   * 
   * @param reuseConnections
   *          whether to reuse connections to brokers or not. If true, won't use {@link ConfigKeys#RECEIVER_HOSTS}.
   * @return new receiver
   */
  public Receiver createReceiver(boolean reuseConnections) {
    List<ConnectionFactory> factories;
    if (reuseConnections) {
      factories = getOrCreateConnectionFactories();
    }
    else {
      factories = createConnectionFactories(RECEIVER_HOSTS);
    }
    return new Receiver(factories, properties.getProperties());
  }

  /**
   * Create new publisher builder.
   * Reuse connections to brokers.
   * Won't use {@link ConfigKeys#PUBLISHER_HOSTS}.
   */
  public PublisherBuilder createPublisherBuilder() {
    return createPublisherBuilder(true);
  }

  /**
   * Create new publisher builder.
   *
   * @param reuseConnections
   *          whether to reuse connections to brokers or not.
   *          If true, won't use {@link ConfigKeys#PUBLISHER_HOSTS}.
   */
  public PublisherBuilder createPublisherBuilder(boolean reuseConnections) {
    List<ConnectionFactory> factories;
    if (reuseConnections) {
      factories = getOrCreateConnectionFactories();
    }
    else {
      factories = createConnectionFactories(PUBLISHER_HOSTS);
    }
    return new PublisherBuilder(factories, properties.getProperties());
  }

  private List<ConnectionFactory> getOrCreateConnectionFactories() {
    if (factories == null) {
      factories = createConnectionFactories(null);
    }
    return factories;
  }

  private Iterable<String> getHosts(String... settingNames) {
    String value;
    for (String settingName : settingNames) {
      value = isEmpty(settingName) ? null : properties.getString(settingName);
      if (!isEmpty(value)) {
        return splitHosts(value);
      }
    }
    throw new ConfigException(String.format("Any of these properties must be set and not empty: %s", Joiner.on(',').join(settingNames)));
  }

  private Iterable<String> splitHosts(String hosts) {
    return Splitter.on(HOSTS_SEPARATOR).split(hosts);
  }

  private List<ConnectionFactory> createConnectionFactories(String hostsSettingName) {
    List<ConnectionFactory> factories = new ArrayList<>();
    try {
      Integer commonPort = properties.getInteger(PORT);
      // something_HOSTS -> HOSTS -> HOST -> exception
      Iterable<String> hosts = getHosts(hostsSettingName, HOSTS, HOST);
      for (String hostAndPortString : hosts) {
        Iterator<String> hostAndPort = Splitter.on(HOSTS_PORT_SEPARATOR).split(hostAndPortString).iterator();
        String host = hostAndPort.next();
        Integer port = commonPort;
        if (hostAndPort.hasNext()) {
          port = Integer.parseInt(hostAndPort.next());
        }
        factories.add(createConnectionFactory(host, port));
      }
      return factories;
    }
    catch (ConfigException e) {
      throw e;
    }
    catch (Exception e) {
      throw new ConfigException("Failed to create connection factories", e);
    }
  }

  private ConnectionFactory createConnectionFactory(String host, Integer port) {
    try {
      CachingConnectionFactory factory = new CachingConnectionFactory();
      factory.setHost(host);
      if (port != null) {
        factory.setPort(port);
      }

      factory.setConnectionTimeout(properties.getInteger(CONNECTION_TIMEOUT_MS, 200));

      factory.setUsername(properties.getNotNullString(USERNAME));
      factory.setPassword(properties.getNotNullString(PASSWORD));
      String virtualhost = properties.getString(VIRTUALHOST);
      if (virtualhost != null) {
        factory.setVirtualHost(virtualhost);
      }

      factory.setRequestedHeartBeat(properties.getInteger(HEARTBIT_SEC, 2));

      Integer channelCacheSize = properties.getInteger(CHANNEL_CACHE_SIZE);
      if (channelCacheSize != null) {
        factory.setChannelCacheSize(channelCacheSize);
      }
      Integer closeTimeout = properties.getInteger(CLOSE_TIMEOUT);
      if (closeTimeout != null) {
        factory.setCloseTimeout(closeTimeout);
      }
      Boolean publisherConfirms = properties.getBoolean(PUBLISHER_CONFIRMS);
      if (publisherConfirms != null) {
        factory.setPublisherConfirms(publisherConfirms);
      }
      Boolean publisherReturns = properties.getBoolean(PUBLISHER_RETURNS);
      if (publisherReturns != null) {
        factory.setPublisherReturns(publisherReturns);
      }
      return factory;
    }
    catch (Exception e) {
      throw new ConfigException(String.format("Failed to create ConnectionFactory (%s)", host), e);
    }
  }

  private boolean isEmpty(String value) {
    return value == null || value.trim().length() == 0;
  }
}
