package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class TaskTrackerSchedulerRunner {
    public static void main(String[] args) {
        SpringApplication.run(TaskTrackerSchedulerRunner.class, args);
    }
}