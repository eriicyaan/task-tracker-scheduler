package com.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.core.io.InputStreamResource;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserReportCreatedEvent {
    private String username;
    private InputStreamResource report;
}
