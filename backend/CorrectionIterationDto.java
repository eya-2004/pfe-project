package com.example.demo;

public class CorrectionIterationDto {

    private Long id;
    private Long ruleId;
    private String toExecute;
    private String createdBy;
    private String dagRunId;
    private String createdAt;
    private Integer iterationId;
    private String detectionRunId;
    private String detectionRuleLabel;
    private String targetColumn;

    public CorrectionIterationDto() {}

    public CorrectionIterationDto(Long id, Long ruleId, String toExecute, String createdBy,
                                  String dagRunId, String createdAt, Integer iterationId,
                                  String detectionRunId, String detectionRuleLabel, String targetColumn) {
        this.id = id;
        this.ruleId = ruleId;
        this.toExecute = toExecute;
        this.createdBy = createdBy;
        this.dagRunId = dagRunId;
        this.createdAt = createdAt;
        this.iterationId = iterationId;
        this.detectionRunId = detectionRunId;
        this.detectionRuleLabel = detectionRuleLabel;
        this.targetColumn = targetColumn;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Long getRuleId() { return ruleId; }
    public void setRuleId(Long ruleId) { this.ruleId = ruleId; }

    public String getToExecute() { return toExecute; }
    public void setToExecute(String toExecute) { this.toExecute = toExecute; }

    public String getCreatedBy() { return createdBy; }
    public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    public String getDagRunId() { return dagRunId; }
    public void setDagRunId(String dagRunId) { this.dagRunId = dagRunId; }

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public Integer getIterationId() { return iterationId; }
    public void setIterationId(Integer iterationId) { this.iterationId = iterationId; }

    public String getDetectionRunId() { return detectionRunId; }
    public void setDetectionRunId(String detectionRunId) { this.detectionRunId = detectionRunId; }

    public String getDetectionRuleLabel() { return detectionRuleLabel; }
    public void setDetectionRuleLabel(String detectionRuleLabel) { this.detectionRuleLabel = detectionRuleLabel; }

    public String getTargetColumn() { return targetColumn; }
    public void setTargetColumn(String targetColumn) { this.targetColumn = targetColumn; }
}