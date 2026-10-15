package uk.gov.companieshouse.disqualifiedofficers.search.consumer;

import consumer.exception.NonRetryableErrorException;
import org.jspecify.annotations.NonNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import uk.gov.companieshouse.disqualifiedofficers.search.processor.ResourceChangedProcessor;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Component
public class DisqualifiedOfficersSearchConsumer {

    private final Logger logger;

    private final ResourceChangedProcessor processor;

    @Autowired
    public DisqualifiedOfficersSearchConsumer(Logger logger, ResourceChangedProcessor processor) {
        super();
        this.logger = logger;
        this.processor = processor;
    }

    /**
     * Receives Main topic messages.
     */
    @RetryableTopic(
            attempts = "${disqualified-officers.search.retry-attempts}",
            backOff = @BackOff(delayString = "${disqualified-officers.search.backoff-delay}"),
            dltTopicSuffix = "${disqualified-officers.search.error-suffix}",
            retryTopicSuffix = "${disqualified-officers.search.retry-suffix}",
            autoCreateTopics = "true",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            exclude = NonRetryableErrorException.class)
    @KafkaListener(
            topics = "${disqualified-officers.search.topic}",
            groupId = "${disqualified-officers.search.group-id}",
            containerFactory = "listenerContainerFactory"
    )
    public void receiveMainMessages(Message<@NonNull ResourceChangedData> message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.info("A new message read from '" + topic + "' topic with payload: " + message.getPayload());
        try {
            processor.processResourceChanged(message);

        } catch (Exception exception) {
            logger.error(String.format("Exception occurred while processing the topic: %s "
                    + "with message: %s", topic, message), exception);
            throw exception;
        }
    }
}
