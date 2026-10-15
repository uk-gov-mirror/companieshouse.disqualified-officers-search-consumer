package uk.gov.companieshouse.disqualifiedofficers.search.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import uk.gov.companieshouse.disqualifiedofficers.search.config.properties.KafkaProperties;
import uk.gov.companieshouse.disqualifiedofficers.search.exception.RetryableTopicErrorInterceptor;
import uk.gov.companieshouse.disqualifiedofficers.search.serialization.ResourceChangedDataDeserializer;
import uk.gov.companieshouse.disqualifiedofficers.search.serialization.ResourceChangedDataSerializer;
import uk.gov.companieshouse.logging.Logger;
import uk.gov.companieshouse.logging.LoggerFactory;
import uk.gov.companieshouse.stream.ResourceChangedData;

@Configuration
@EnableKafka
@Profile("!test")
public class KafkaConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger("disqualified-officers-search-consumer");

    private final ResourceChangedDataDeserializer resourceChangedDeserializer;
    private final ResourceChangedDataSerializer resourceChangedSerializer;

    private final KafkaProperties kafkaProperties;

    /**
     * Constructor.
     */
    public KafkaConfig(ResourceChangedDataDeserializer resourceChangedDeserializer,
                       ResourceChangedDataSerializer resourceChangedSerializer,
                       KafkaProperties kafkaProperties) {
        this.resourceChangedDeserializer = resourceChangedDeserializer;
        this.resourceChangedSerializer = resourceChangedSerializer;
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Kafka Consumer Factory.
     */
    @Bean
    public ConsumerFactory<@NonNull String, ResourceChangedData> kafkaConsumerFactory() {
        LOGGER.info("kafkaConsumerFactory() method called.");

        return new DefaultKafkaConsumerFactory<>(consumerConfigs(), new StringDeserializer(),
                new ErrorHandlingDeserializer<>(resourceChangedDeserializer));
    }

    /**
     * Kafka Producer Factory.
     */
    @Bean
    public ProducerFactory<@NonNull String, Object> producerFactory() {
        LOGGER.info("producerFactory() method called.");

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ResourceChangedDataSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, RetryableTopicErrorInterceptor.class.getName());

        // Add authentication properties from KafkaProperties
        props.putAll(kafkaProperties.getAuthentication());

        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), resourceChangedSerializer);
    }

    @Bean
    public KafkaTemplate<@NonNull String, @NonNull Object> kafkaTemplate() {
        LOGGER.info("kafkaTemplate() method called.");

        return new KafkaTemplate<>(producerFactory());
    }

    /**
     * Kafka Listener Container Factory.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<@NonNull String, @NonNull ResourceChangedData> listenerContainerFactory() {
        LOGGER.info("listenerContainerFactory() method called.");

        var factory = new ConcurrentKafkaListenerContainerFactory<@NonNull String, @NonNull ResourceChangedData>();
        factory.setConsumerFactory(kafkaConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }

    private Map<String, Object> consumerConfigs() {
        LOGGER.info("consumerConfigs() method called.");

        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, ResourceChangedDataDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Add authentication properties from KafkaProperties
        props.putAll(kafkaProperties.getAuthentication());

        return props;
    }
}