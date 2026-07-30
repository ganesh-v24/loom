package com.workflowengine.api;

import com.workflowengine.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** api-service produces to these two topics; the three it only consumes are declared elsewhere. */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic instanceLifecycleTopic() {
        return new NewTopic(Topics.INSTANCE_LIFECYCLE, 3, (short) 1);
    }

    @Bean
    public NewTopic definitionPublishedTopic() {
        return new NewTopic(Topics.DEFINITION_PUBLISHED, 3, (short) 1);
    }
}
