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

    @Column(name = "row_limit")
    private Integer rowLimit;

    @Column(name = "offset_current")
    private Integer offsetCurrent = 0;

    @Column(name = "created_by", length = 100)
    private String createdBy;

    @Column(name = "dag_run_id", length = 255)
    private String dagRunId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    public enum ToExecuteStatus {
        YES, NO
    }
}