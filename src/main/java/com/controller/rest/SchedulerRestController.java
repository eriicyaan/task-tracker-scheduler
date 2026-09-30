package com.controller.rest;


import com.dto.response.UserResponse;
import com.kafka.events.UserReportCreatedEvent;
import com.kafka.rpc.summarization.SchedulerSummarizationRequest;
import com.kafka.rpc.summarization.SchedulerSummarizationResponse;
import lombok.RequiredArgsConstructor;
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

@RestController
@RequestMapping("/api/schedular")
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


        for(UserResponse user : users) {
            SchedulerSummarizationResponse taskSummarizationResponse = generateReport(user.id());

            UserReportCreatedEvent userReportCreatedEvent = new UserReportCreatedEvent(
                    user.username(),
                    taskSummarizationResponse.getResource()
            );

             kafkaTemplate.send("email-sending-tasks", userReportCreatedEvent);
        }

    }


    private List<UserResponse> getUsers() {
        RestClient rest = RestClient.builder()
                .baseUrl("http://localhost:8081/api/internal/backend/users")
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
