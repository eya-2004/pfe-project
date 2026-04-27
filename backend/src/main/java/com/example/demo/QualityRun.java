package com.example.demo;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "bscs_quality_run")
public class QualityRun {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "run_id", unique = true)
    private String runId;

    @Column(name = "iteration_id")  // ← AJOUTER CE CHAMP
    private Integer iterationId;      // ← AJOUTER CE CHAMP

    @Column(name = "execution_date")
    private LocalDateTime executionDate;

    @Column(name = "triggered_by")
    private String triggeredBy;

    @Column(name = "nb_tables")
    private Integer nbTables;

    @Column(name = "nb_rules")
    private Integer nbRules;

    @Column(name = "nb_violations")
    private Integer nbViolations;

    @Column(name = "nb_rows_with_violation")
    private Integer nbRowsWithViolation;

    @Column(name = "quality_score")
    private Float qualityScore;

    @Column(name = "quality_score_weighted")
    private Float qualityScoreWeighted;

    @Column(name = "status")
    private String status;

    @Column(name = "error_message")
    private String errorMessage;

    public QualityRun() {}

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }

    // ← AJOUTER GETTER ET SETTER POUR iterationId
    public Integer getIterationId() { return iterationId; }
    public void setIterationId(Integer iterationId) { this.iterationId = iterationId; }

    public LocalDateTime getExecutionDate() { return executionDate; }
    public void setExecutionDate(LocalDateTime executionDate) { this.executionDate = executionDate; }

    public String getTriggeredBy() { return triggeredBy; }
    public void setTriggeredBy(String triggeredBy) { this.triggeredBy = triggeredBy; }

    public Integer getNbTables() { return nbTables; }
    public void setNbTables(Integer nbTables) { this.nbTables = nbTables; }

    public Integer getNbRules() { return nbRules; }
    public void setNbRules(Integer nbRules) { this.nbRules = nbRules; }

    public Integer getNbViolations() { return nbViolations; }
    public void setNbViolations(Integer nbViolations) { this.nbViolations = nbViolations; }

    public Integer getNbRowsWithViolation() { return nbRowsWithViolation; }
    public void setNbRowsWithViolation(Integer nbRowsWithViolation) {
        this.nbRowsWithViolation = nbRowsWithViolation;
    }

    public Float getQualityScore() { return qualityScore; }
    public void setQualityScore(Float qualityScore) { this.qualityScore = qualityScore; }

    public Float getQualityScoreWeighted() { return qualityScoreWeighted; }
    public void setQualityScoreWeighted(Float qualityScoreWeighted) {
        this.qualityScoreWeighted = qualityScoreWeighted;
    }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
}