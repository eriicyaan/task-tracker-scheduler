package com.dto.response;

import org.springframework.core.io.InputStreamResource;

import java.util.UUID;

public record TaskSummarizationResponse(UUID userId,
                                        InputStreamResource resource) {
}
