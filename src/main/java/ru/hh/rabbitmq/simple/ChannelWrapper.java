package ru.hh.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.hh.rabbitmq.ChannelFactory;
import ru.hh.rabbitmq.ConnectionFailedException;
import ru.hh.rabbitmq.TransactionException;
import ru.hh.rabbitmq.impl.AutoreconnectProperties;

public class ChannelWrapper {
  private static final Logger logger = LoggerFactory.getLogger(ChannelWrapper.class);

  private QueueProperties queueProperties;
  private ExchangeProperties exchangeProperties;
  private String routingKey;
  private ChannelFactory factory;
  private AutoreconnectProperties autoreconnect;
  private boolean transactional;

  private boolean nonEmptyTransaction;
  private boolean closed;

  private Channel channel;

  public ChannelWrapper(QueueProperties properties, boolean transactional, ChannelFactory factory) {
    this(null, properties, null, transactional, factory, null);
  }

  public ChannelWrapper(String queue, ChannelFactory factory, AutoreconnectProperties autoreconnect) {
    this(null, new QueueProperties(queue, false), null, false, factory, autoreconnect);
  }

  public ChannelWrapper(ExchangeProperties properties, boolean transactional, ChannelFactory factory) {
    this(properties, null, null, transactional, factory, null);
  }

  public ChannelWrapper(String queue, boolean durable, boolean transactional, ChannelFactory factory) {
    this(null, new QueueProperties(queue, durable), null, transactional, factory, null);
  }

  public ChannelWrapper(
      ExchangeProperties exchangeProperties, QueueProperties queueProperties, String routingKey, boolean transactional,
      ChannelFactory factory, AutoreconnectProperties autoreconnect) {
    this.queueProperties = queueProperties;
    this.exchangeProperties = exchangeProperties;
    this.routingKey = routingKey;
    this.transactional = transactional;
    this.factory = factory;
    this.autoreconnect = autoreconnect == null ? new AutoreconnectProperties(0) : autoreconnect;
  }

  public void commit() {
    ensureConnectedAndRunning();
    try {
      channel.txCommit();
      nonEmptyTransaction = false;
    } catch (IOException e) {
      throw new TransactionException("Error commiting transaction", e);
    }
  }

  public void rollback() {
    ensureConnectedAndRunning();
    try {
      channel.txRollback();
      // TODO: beware of channel remaining in transactional state here (see amqp specs)
      nonEmptyTransaction = false;
    } catch (IOException e) {
      throw new TransactionException("Error rolling back transaction", e);
    }
  }

  public void send(ReturnListener returnListener, Message... message) throws IOException {
    ensureConnectedAndRunning();
    setReturnListener(returnListener);
    send(message);
  }

  private String getTargetExchangeName() {
    if (exchangeProperties != null) {
      return exchangeProperties.getName();
    }
    return "";
  }

  private String getTargetRoutingKey() {
    if (routingKey != null) {
      return routingKey;
    }
    return queueProperties.getName();
  }

  public void send(Message... messages) throws IOException {
    ensureConnectedAndRunning();
    for (Message message : messages) {
      channel.basicPublish(
        getTargetExchangeName(), getTargetRoutingKey(), true, false, message.getProperties(), message.getBody());
      nonEmptyTransaction = true;
    }
  }

  public void send(Collection<Message> messages) throws IOException {
    for (Message message : messages) {
      send(message);
    }
  }

  public void send(ReturnListener returnListener, Collection<Message> messages) throws IOException {
    ensureConnectedAndRunning();
    setReturnListener(returnListener);
    send(messages);
  }

  public void setReturnListener(ReturnListener returnListener) {
    if (returnListener != null) {
      channel.setReturnListener(returnListener);
    }
  }

  public void resetReturnListener() {
    channel.setReturnListener(null);
  }

  /**
   * Receives and processes single message from the queue. This method does not block the caller and returns immediately.
   *
   * @param  receiver  receiver implementation that will be used to process incoming message
   *
   * @return  true if the queue returned a message, false if the queue was empty at the time of calling
   *
   * @throws  IOException
   * @throws  InterruptedException
   */
  public boolean receiveSingle(MessageReceiver receiver) throws IOException, InterruptedException {
    ensureConnectedAndRunning();
    GetResponse response = channel.basicGet(queueProperties.getName(), false);
    if (response == null) {
      return false;
    }
    Message message = Message.fromGetResponse(response);
    receiver.receive(message);
    long deliveryTag = response.getEnvelope().getDeliveryTag();
    channel.basicAck(deliveryTag, false);
    nonEmptyTransaction = true;
    return true;
  }

  /**
   * Receives messages from queue, waits (blocks) until queue returns next message. Stops when supplied receiver's
   * {@link MessagesReceiver#isEnough()} returns true, if timeout has been reached or thread is interrupted.
   *
   * @param  receiver  receiver implementation that will be used to process incoming message
   * @param  timeout  max time to wait in blocking state
   *
   * @throws  IOException
   * @throws  ShutdownSignalException
   * @throws  InterruptedException
   */
  public void waitAndReceiveMany(MessagesReceiver receiver, Long timeout) throws IOException, ShutdownSignalException,
    InterruptedException {
    ensureConnectedAndRunning();
    if (Thread.currentThread().isInterrupted()) {
      return;
    }
    QueueingConsumer consumer = new QueueingConsumer(channel);
    String consumerTag = channel.basicConsume(queueProperties.getName(), false, consumer);
    Delivery delivery;
    Message message;
    try {
      receiver.onStart();
      do {
        if (timeout != null) {
          delivery = consumer.nextDelivery(timeout);
        } else {
          delivery = consumer.nextDelivery();
        }
        if (delivery == null) {
          break;
        }
        message = Message.fromDelivery(delivery);

        if (Thread.currentThread().isInterrupted()) {
          return;
        }

        receiver.receive(message);

        // if we got the message and processed it we need to send ack even if thread was interrupted
        // so we save interrupted flag after receiver action and restore it after ack action because sometimes RabbitMQ resets it somewhere inside.
        boolean interrupted = Thread.currentThread().isInterrupted();
        long deliveryTag = delivery.getEnvelope().getDeliveryTag();
        channel.basicAck(deliveryTag, false);
        nonEmptyTransaction = true;

        if (interrupted && !Thread.currentThread().isInterrupted()) {
          Thread.currentThread().interrupt();
        }
      } while (!receiver.isEnough() && !Thread.currentThread().isInterrupted());
    } finally {
      channel.basicCancel(consumerTag);
      receiver.onFinish();
    }
  }

  public void purge() throws IOException {
    ensureConnectedAndRunning();
    channel.queuePurge(queueProperties.getName());
  }

  public void close() {
    closed = true;
    factory.returnChannel(channel);
  }

  private void ensureConnectedAndRunning() {
    if (closed) {
      throw new IllegalStateException("Already closed");
    }
    ensureConnected();
  }

  private void ensureConnected() {
    if (transactional && nonEmptyTransaction) {
      // ignore reconnection attempt, let closed connection throw it's own exception
      return;
    }

    String queueName = queueProperties != null ? queueProperties.getName() : null;
    boolean queueDurable = queueProperties != null ? queueProperties.isDurable() : false;

    String exchangeName = exchangeProperties != null ? exchangeProperties.getName() : null;
    String exchangeType = exchangeProperties != null ? exchangeProperties.getType() : null;
    boolean exchangeDurable = exchangeProperties != null ? exchangeProperties.isDurable() : false;

    int attempt = 0;
    while (channel == null || !channel.isOpen()) {
      attempt++;
      try {
        logger.debug("Openning channel");
        channel = factory.openChannel(exchangeName, exchangeType, exchangeDurable, queueName, queueDurable, routingKey);
        if (transactional) {
          channel.txSelect();
        }
        logger.debug("Channel is ready");
      } catch (IOException e) {
        if (attempt > autoreconnect.getAttempts()) {
          throw new ConnectionFailedException("Can't open channel", e);
        }
        logger.warn(
          String.format(
            "Attempt %d out of %d to reconnect the channel has failed, sleeping then retrying", attempt,
            autoreconnect.getAttempts()), e);
        try {
          autoreconnect.getSleeper().sleep();
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
          throw new ConnectionFailedException("Sleep between autoreconnection attempts has been interrupted", e1);
        }
      }
    } // while not connected
  }
}
