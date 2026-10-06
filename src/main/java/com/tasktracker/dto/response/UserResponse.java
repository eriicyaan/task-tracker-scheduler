package com.tasktracker.dto.response;

import java.util.UUID;

public record UserResponse(UUID id,
                           String username) {
}
