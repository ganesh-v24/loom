package com.workflowengine.engine;

import com.workflowengine.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic stepCompletedTopic() {
        return new NewTopic(Topics.STEP_COMPLETED, 3, (short) 1);
    }

    @Bean
    public NewTopic stepDlqTopic() {
        return new NewTopic(Topics.STEP_DLQ, 1, (short) 1);
    }

    @Bean
    public NewTopic notificationRequestedTopic() {
        return new NewTopic(Topics.NOTIFICATION_REQUESTED, 3, (short) 1);
    }

    @Bean
    public NewTopic compensationCompletedTopic() {
        return new NewTopic(Topics.COMPENSATION_COMPLETED, 1, (short) 1);
    }

    @Bean
    public NewTopic stepExecutionRecordedTopic() {
        return new NewTopic(Topics.STEP_EXECUTION_RECORDED, 3, (short) 1);
    }

    /** Infra-level dead-lettering for unexpected exceptions escaping a @KafkaListener here. */
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }
}
