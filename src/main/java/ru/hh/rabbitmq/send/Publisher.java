package ru.hh.rabbitmq.send;

import com.google.common.base.Service;
import com.rabbitmq.client.Address;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ConnectionFactory;
import ru.hh.rabbitmq.impl.ChannelFactoryImpl;
import ru.hh.rabbitmq.impl.SingleConnectionFactory;
import ru.hh.rabbitmq.simple.Message;
import ru.hh.rabbitmq.util.Addresses;

public class Publisher {
  private static final Logger logger = LoggerFactory.getLogger(Publisher.class);
  
  private final ConnectionFactory[] connectionFactories;
  private final Service[] workers;
  private final BlockingQueue<ChannelTask> taskQueue;

  public Publisher(com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay,
                   int attempts, int queueLength, Address... addresses) {
    if (addresses.length < 1) {
      throw new IllegalArgumentException("can't create Publisher without connection addresses");
    }
    connectionFactories = new ConnectionFactory[addresses.length];
    workers = new Service[addresses.length];
    taskQueue = new ArrayBlockingQueue<ChannelTask>(queueLength);
    for(int i = 0; i < addresses.length; i ++) {
      connectionFactories[i] = new SingleConnectionFactory(connectionFactory, retryUnit, retryDelay, attempts, addresses[i]);
      workers[i] = new ChannelWorker(new ChannelFactoryImpl(connectionFactories[i]), taskQueue, addresses[i].toString() + "-publisher-worker");
      workers[i].start();
    }
  }

  public Publisher(com.rabbitmq.client.ConnectionFactory connectionFactory, TimeUnit retryUnit, long retryDelay,
                   int attempts, int queueLength, String hosts, int port) {
    this(connectionFactory, retryUnit, retryDelay, attempts, queueLength, Addresses.split(hosts, port));
  }

  public void close() {
    for (Service worker : workers) {
      worker.stopAndWait();
    }
    for(ConnectionFactory factory : connectionFactories) {
      factory.close();
    }
  }

  /**
   * Fast sending method, enqueues messages internally, throws exception if local queue full
   * 
   * @return Future that gets completed after successful sending
   */
  public Future<Void> send(final Destination destination, final Collection<Message> messages) {
    ChannelTaskFuture future = new ChannelTaskFuture(new PublishTask(destination, messages, false));
    taskQueue.add(future);
    return future;
  }

  /**
   * Blocking transactional sending method, enqueues messages internally, waiting if necessary
   * for space to become available, then waiting for operation to complete
   */
  public void sendTransactional(long timeout, TimeUnit unit, Destination destination, Collection<Message> messages) {
    
  }
}
