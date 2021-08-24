package ru.hh.rabbitmq.spring.send;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import ru.hh.nab.metrics.Counters;
import ru.hh.nab.metrics.StatsDSender;
import ru.hh.nab.metrics.Tag;

public class MessageSender {

  private final RabbitTemplate template;
  @Nullable
  private final Counters publishedCounters;
  @Nullable
  private final Counters errorsCounters;
  @Nullable
  private final Tag app;

  public MessageSender(RabbitTemplate template,
                @Nullable
                String serviceName,
                @Nullable
                StatsDSender statsDSender) {
    this.template = template;
    if (statsDSender != null) {
      publishedCounters = new Counters(20);
      errorsCounters = new Counters(20);
      app = new Tag("app", serviceName);

      statsDSender.sendPeriodically(() -> {
        statsDSender.sendCounters("rabbit.publishers.messages", publishedCounters);
        statsDSender.sendCounters("rabbit.publishers.errors", errorsCounters);
      });

    } else {
      publishedCounters = null;
      errorsCounters = null;
      app = null;
    }
  }

  public void publishMessages(Map<?, ? extends Destination> messages) {
    for (Map.Entry<?, ? extends Destination> entry : messages.entrySet()) {
      Object message = entry.getKey();
      Destination destination = entry.getValue();
      publishMessage(message, destination);
    }
  }

  public void publishMessage(Object message, Destination destination) {
    CorrelationData correlationData = null;
    if (message instanceof CorrelatedMessage) {
      CorrelatedMessage correlated = (CorrelatedMessage) message;
      correlationData = correlated.getCorrelationData();
      message = correlated.getMessage();
    }

    String exchange = Optional.ofNullable(destination).map(Destination::getExchange).orElseGet(template::getExchange);
    String routingKey = Optional.ofNullable(destination).map(Destination::getRoutingKey).orElseGet(template::getRoutingKey);
    String host = template.getConnectionFactory().getHost() + ":" + template.getConnectionFactory().getPort();

    try {
      if (destination != null && destination.getRoutingKey() != null) {
          template.convertAndSend(destination.getExchange(), destination.getRoutingKey(), message, correlationData);
      } else {
          template.correlationConvertAndSend(message, correlationData);
      }
    } catch (AmqpException e) {
      if (errorsCounters != null) {
        addValueToCountersWithDestinationTag(errorsCounters, app, exchange, routingKey, host);
      }
      throw e;
    } finally {
      if (publishedCounters != null) {
        // deliberately not sending exchange and host to avoid overloading okmeter
        addValueToCountersWithDestinationTag(publishedCounters, app, null, routingKey, null);
      }
    }
  }

  RabbitTemplate getTemplate() {
    return template;
  }

  private static void addValueToCountersWithDestinationTag(Counters counters, Tag app, String exchange, String routingKey, String host) {
    List<Tag> tags = new ArrayList<>();
    tags.add(app);
    Optional.ofNullable(exchange).map(v -> new Tag("exchange", v)).ifPresent(tags::add);
    Optional.ofNullable(host).map(v -> new Tag("host", v)).ifPresent(tags::add);
    tags.add(new Tag("routing_key", Optional.ofNullable(routingKey).orElse("unknown")));

    counters.add(1, tags.toArray(new Tag[0]));
  }
}
