package com.tasktracker.controller.rest;


import com.tasktracker.dto.response.UserResponse;
import com.tasktracker.kafka.events.EmailSendingEvent;
import com.tasktracker.kafka.events.EventType;
import com.tasktracker.kafka.rpc.summarization.SchedulerSummarizationRequest;
import com.tasktracker.kafka.rpc.summarization.SchedulerSummarizationResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@RestController
@RequestMapping("/api/scheduler")
@RequiredArgsConstructor
public class SchedulerRestController {

    @Value("${internal.service.secret}")
    private String secret;

    private final ReplyingKafkaTemplate<
            UUID,
            SchedulerSummarizationRequest,
            SchedulerSummarizationResponse> replyingKafkaTemplate;

    private final KafkaTemplate<UUID, Object> kafkaTemplate;


    @GetMapping
    public void doSchedular() throws ExecutionException, InterruptedException {
        List<UserResponse> users = getUsers();
        log.info("RECEIVE USERS: {}", users);

        for(UserResponse user : users) {
            SchedulerSummarizationResponse taskSummarizationResponse = generateReport(user.id());

            log.info("GENERATE REPORT: {}", taskSummarizationResponse);

            EmailSendingEvent emailSendingEvent = EmailSendingEvent.builder()
                    .id(user.id())
                    .username(user.username())
                    .report(taskSummarizationResponse.getResource())
                    .eventType(EventType.USER_REPORT_CREATED)
                    .build();

            log.info("SEND TO KAFKA TOPIC MESSAGE: {}", emailSendingEvent);
            kafkaTemplate.send("email-sending-tasks", emailSendingEvent);
        }

    }


    private List<UserResponse> getUsers() {
        RestClient rest = RestClient.builder()
                .baseUrl("http://task-tracker-backend:8080/api/internal/backend/users")
                .defaultHeader("X-Internal-Service-Key", secret)
                .build();


        return rest.get()
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });
    }


    private SchedulerSummarizationResponse generateReport(UUID id) throws ExecutionException, InterruptedException {

        SchedulerSummarizationRequest request = new SchedulerSummarizationRequest(id);

        ProducerRecord<UUID, SchedulerSummarizationRequest> record =
                new ProducerRecord<>(
                        "schedular-summarization-request-topic",
                        UUID.randomUUID(),
                        request
                );


        RequestReplyFuture<
                UUID,
                SchedulerSummarizationRequest,
                SchedulerSummarizationResponse
                > future = replyingKafkaTemplate.sendAndReceive(record);


        ConsumerRecord<UUID, SchedulerSummarizationResponse> response = future.get();

        return response.value();
    }

}
