package com.workflowengine.engine;

import com.workflowengine.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
@EnableScheduling
public class KafkaTopicConfig {

    @Bean
    public NewTopic stepExecuteTopic() {
        return new NewTopic(Topics.STEP_EXECUTE, 3, (short) 1);
    }

    @Bean
    public NewTopic compensationRequestedTopic() {
        return new NewTopic(Topics.COMPENSATION_REQUESTED, 1, (short) 1);
    }

    @Bean
    public NewTopic instanceStateChangedTopic() {
        return new NewTopic(Topics.INSTANCE_STATE_CHANGED, 3, (short) 1);
    }

    @Bean
    public NewTopic auditEventOccurredTopic() {
        return new NewTopic(Topics.AUDIT_EVENT_OCCURRED, 3, (short) 1);
    }

    /**
     * Explicitly defined rather than left to Boot's autoconfiguration: once this class also
     * defines outboxKafkaTemplate below, Spring's @ConditionalOnMissingBean(KafkaTemplate.class)
     * check for Boot's own default template sees a KafkaTemplate bean already exists (generics
     * are erased at runtime, so KafkaTemplate<String,String> satisfies a raw-type check just as
     * well as KafkaTemplate<Object,Object> would) and backs off — so the default never gets
     * created unless it's provided here too.
     */
    @Bean
    public ProducerFactory<Object, Object> kafkaProducerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory) {
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    /** Infra-level dead-lettering for unexpected exceptions escaping a @KafkaListener here. */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }

    /**
     * A separate plain-String producer just for OutboxRelay: outbox payloads are already
     * JSON-serialized text, so sending them through the JsonSerializer-configured default
     * KafkaTemplate<Object,Object> (used by @KafkaListener error handling above) would
     * double-encode them.
     */
    @Bean
    public ProducerFactory<String, String> outboxProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> outboxKafkaTemplate(ProducerFactory<String, String> outboxProducerFactory) {
        return new KafkaTemplate<>(outboxProducerFactory);
    }
}
