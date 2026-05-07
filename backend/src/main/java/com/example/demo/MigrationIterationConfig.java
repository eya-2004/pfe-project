package com.example.demo;

import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "correction_iteration_config")
@Data
public class MigrationIterationConfig {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "iteration_id")  // ✅ NOUVELLE COLONNE
    private Integer iterationId;

    @Column(name = "rule_id", nullable = false)
    private Long ruleId;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_execute", length = 3)
    private ToExecuteStatus toExecute = ToExecuteStatus.YES;


    @Column(name = "created_by", length = 100)
    private String createdBy;

    @Column(name = "dag_run_id", length = 255)
    private String dagRunId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
    @Column(name = "detection_run_id")
    private String detectionRunId;
    @Column(name = "detection_rule_label", length = 500)
    private String detectionRuleLabel;

    @Column(name = "target_column", length = 255)
    private String targetColumn;
    public enum ToExecuteStatus {
        YES, NO
    }
}