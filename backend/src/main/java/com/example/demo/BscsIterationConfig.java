package com.example.demo;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "BSCS_ITERATION_CONFIG")
public class BscsIterationConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "iteration_id", nullable = false)
    private Integer iterationId;

    @Column(name = "run_id", length = 200)
    private String runId;

    @Column(name = "table_name", nullable = false, length = 200)
    private String tableName;

    @Column(name = "column_name", nullable = false, length = 200)
    private String columnName;

    @Column(name = "rule", nullable = false, length = 500)
    private String rule;

    @Column(name = "flag_to_check")
    private Boolean flagToCheck;

    @Column(name = "selected_by", length = 100)
    private String selectedBy;

    @Column(name = "selected_at")
    private LocalDateTime selectedAt;
    @Column(name = "batch_size")
    private Integer batchSize;
    @Column(name = "rule_id")
    private Integer ruleId;
    @Column(name = "offset_current")
    private Integer offsetCurrent; 
    @Column(name = "rule_origin")
    private String ruleOrigin;
    @Column(name = "has_correction", nullable = false)
private Boolean hasCorrection = false;
    // Constructeurs
    public BscsIterationConfig() {}

    public BscsIterationConfig(Integer iterationId, String tableName, String columnName,
                           String rule, Boolean flagToCheck, String selectedBy) {
        this.iterationId = iterationId;
        this.tableName = tableName;
        this.columnName = columnName;
        this.rule = rule;
        this.flagToCheck = flagToCheck;
        this.selectedBy = selectedBy;
        this.selectedAt = LocalDateTime.now();
    }

    // Getters et Setters
    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public Integer getIterationId() { return iterationId; }
    public void setIterationId(Integer iterationId) { this.iterationId = iterationId; }

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getColumnName() { return columnName; }
    public void setColumnName(String columnName) { this.columnName = columnName; }

    public String getRule() { return rule; }
    public void setRule(String rule) { this.rule = rule; }

    public Boolean getFlagToCheck() { return flagToCheck; }
    public void setFlagToCheck(Boolean flagToCheck) { this.flagToCheck = flagToCheck; }

    public String getSelectedBy() { return selectedBy; }
    public void setSelectedBy(String selectedBy) { this.selectedBy = selectedBy; }

    public LocalDateTime getSelectedAt() { return selectedAt; }
    public void setSelectedAt(LocalDateTime selectedAt) { this.selectedAt = selectedAt; }
    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }
    public Integer getRuleId() {
        return ruleId;
    }

    public void setRuleId(Integer ruleId) {
        this.ruleId = ruleId;
    }
    public String getRuleOrigin() { return ruleOrigin; }
    public void setRuleOrigin(String ruleOrigin) { this.ruleOrigin = ruleOrigin; }
    public Integer getOffsetCurrent() {
        return offsetCurrent;
    }

    public void setOffsetCurrent(Integer offsetCurrent) {
        this.offsetCurrent = offsetCurrent;
    }

   

    public Boolean getHasCorrection() { return hasCorrection; }
    public void setHasCorrection(Boolean hasCorrection) { this.hasCorrection = hasCorrection; }
}
