package com.tasktracker.configuration;


import com.tasktracker.kafka.rpc.summarization.SchedulerSummarizationRequest;
import com.tasktracker.kafka.rpc.summarization.SchedulerSummarizationResponse;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Configuration
@RequiredArgsConstructor
public class KafkaConfiguration {


    private final Environment environment;


    @Bean
    public KafkaTemplate<UUID, Object> kafkaTemplate(ProducerFactory<UUID, Object> producerObjectFactory) {
        return new KafkaTemplate<>(producerObjectFactory);
    }

    @Bean
    public ReplyingKafkaTemplate<UUID, SchedulerSummarizationRequest, SchedulerSummarizationResponse> replyingKafkaTemplate(
            ProducerFactory<UUID, SchedulerSummarizationRequest> producerFactory,
            ConcurrentMessageListenerContainer<UUID, SchedulerSummarizationResponse> repliesContainer) {
        var replyingKafkaTemplate = new ReplyingKafkaTemplate<>(producerFactory, repliesContainer);

        replyingKafkaTemplate.setDefaultReplyTimeout(Duration.ofMinutes(5));
        return replyingKafkaTemplate;
    }



    @Bean
    public ConcurrentMessageListenerContainer<UUID, SchedulerSummarizationResponse> repliesContainer(
            ConsumerFactory<UUID, SchedulerSummarizationResponse> consumerFactory) {
        ContainerProperties containerProperties =
                new ContainerProperties("schedular-summarization-response-topic");


        return new ConcurrentMessageListenerContainer<>(
                consumerFactory,
                containerProperties
        );
    }

    @Bean
    public NewTopic schedularSummarizationRequestTopic() {
        return TopicBuilder
                .name("scheduler-summarization-request-topic")
                .partitions(3)
                .build();
    }

    @Bean
    public NewTopic schedularSummarizationResponseTopic() {
        return TopicBuilder
                .name("scheduler-summarization-response-topic")
                .partitions(3)
                .build();
    }


    @Bean
    ProducerFactory<UUID, Object> producerObjectFactory() {
        return new DefaultKafkaProducerFactory<>(getProducerConfig());
    }


    @Bean
    ProducerFactory<UUID, SchedulerSummarizationRequest> producerFactory() {
        return new DefaultKafkaProducerFactory<>(getProducerConfig());
    }

    @Bean
    ConsumerFactory<UUID, SchedulerSummarizationResponse> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getConsumerConfig());
    }


    private Map<String, Object> getConsumerConfig() {
        Map<String, Object> config = new HashMap<>();

        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.consumer.bootstrap-servers"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));
        config.put(JacksonJsonDeserializer.TRUSTED_PACKAGES, environment.getProperty("spring.kafka.consumer.trusted-packages"));

        return config;
    }

    private Map<String, Object> getProducerConfig() {
        HashMap<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.producer.bootstrap-servers"));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, environment.getProperty("spring.kafka.producer.acks"));
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, environment.getProperty("spring.kafka.producer.properties.enable.idempotence"));

        return config;
    }
}