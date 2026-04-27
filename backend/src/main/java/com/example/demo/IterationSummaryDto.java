package com.example.demo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class IterationSummaryDto {
    private Integer iterationId;
    private String runId;
    private String selectedAt;

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public IterationSummaryDto(Integer iterationId, String runId, LocalDateTime selectedAt) {
        this.iterationId = iterationId;
        this.runId = runId;
        this.selectedAt = selectedAt != null ? selectedAt.format(FORMATTER) : null;
    }

    public Integer getIterationId() { return iterationId; }
    public String getRunId() { return runId; }
    public String getSelectedAt() { return selectedAt; }
}