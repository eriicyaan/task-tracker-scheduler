package com.controller.rest;


import com.dto.response.TaskSummarizationResponse;
import com.dto.response.UserResponse;
import com.event.UserReportCreatedEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/schedular")
@RequiredArgsConstructor
public class SchedularRestController {

    @Value("${internal.service.secret}")
    private String secret;

    @GetMapping
    public void doSchedular() {
        List<UserResponse> users = getUsers();


        for(UserResponse user : users) {
            TaskSummarizationResponse taskSummarizationResponse = generateReport(user.id());

            UserReportCreatedEvent userReportCreatedEvent = new UserReportCreatedEvent(
                    user.username(),
                    taskSummarizationResponse.resource()
            );

            // kafkaTemplate.send("email-sending-tasks", userReportCreatedEvent);
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


    private TaskSummarizationResponse generateReport(UUID id) {
        RestClient rest = RestClient.builder()
                .baseUrl("http://localhost:8081/api/internal/summarization/" + id)
                .defaultHeader("X-Internal-Service-Key", secret)
                .build();


        ResponseEntity<TaskSummarizationResponse> report = rest.get()
                .retrieve()
                .body(new ParameterizedTypeReference<>() {
                });

        return report.getBody();
    }

}
