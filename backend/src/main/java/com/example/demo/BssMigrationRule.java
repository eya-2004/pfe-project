package com.example.demo;


import jakarta.persistence.*;
import lombok.Data;
import java.time.LocalDateTime;

@Entity
@Table(name = "bss_migration_rules")
@Data
public class BssMigrationRule {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "rule_id")
    private Long ruleId;


    @Column(name = "source_table", length = 100)
    private String sourceTable;

    @Column(name = "source_column", length = 100)
    private String sourceColumn;

    @Column(name = "rule_label", length = 255)
    private String ruleLabel;

    @Column(name = "rule_type", length = 50)
    private String ruleType;

    @Column(name = "default_value", columnDefinition = "TEXT")
    private String defaultValue;

    @Column(name = "created_at")
    private LocalDateTime createdAt;
}