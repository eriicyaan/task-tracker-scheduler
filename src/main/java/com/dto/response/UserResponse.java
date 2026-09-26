package com.dto.response;

import java.util.UUID;

public record UserResponse(UUID id,
                           String username) {
}
