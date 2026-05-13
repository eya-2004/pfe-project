package com.example.demo;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.math.BigDecimal;

/**
 * Entité mappée sur BSCS_DETECTED_INCONSISTENCY.
 * Tous les champs numériques (nb_violations, taux_erreur, nb_to_correct, nb_to_migrate)
 * sont calculés dans le DAG Airflow — aucun calcul ici.
 */
@Entity
@Table(name = "BSCS_DETECTED_INCONSISTENCY")
public class Inconsistency {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "run_id")
    private String runId;

    @Column(name = "iteration_id")
    private Integer iterationId;

    @Column(name = "execution_date")
    private LocalDateTime executionDate;

    @Column(name = "table_name")
    private String tableName;

    @Column(name = "column_name")
    private String columnName;

    @Column(name = "rule")
    private String rule;

    @Column(name = "rule_description", columnDefinition = "TEXT")
    private String ruleDescription;

    @Column(name = "error_category")
    private String errorCategory;

    /** Nombre total de lignes dans la table source */
    @Column(name = "count_source")
    private Integer countSource;

    /** Nombre de LIGNES DISTINCTES en erreur pour cette règle (calculé dans le DAG) */
    @Column(name = "nb_violations")
    private Integer nbViolations;

    /** Lignes à corriger = nb_violations (calculé dans le DAG) */
    @Column(name = "nb_to_correct")
    private Integer nbToCorrect;

    /** Lignes saines = count_source - nb_violations (calculé dans le DAG) */
    @Column(name = "nb_to_migrate")
    private Integer nbToMigrate;


    @Column(name = "taux_rejet", precision = 6, scale = 2)
    private BigDecimal tauxRejet;
    @Column(name = "nb_to_correct_after")
    private Integer nbToCorrectAfter;

    @Column(name = "nb_violations_after")
    private Integer nbViolationsAfter;

    @Column(name = "taux_rejet_after", precision = 6, scale = 2)
    private BigDecimal tauxRejetAfter;
    @Column(name = "nb_to_migrate_after")
    private Integer nbToMigrateAfter;

    @Column(name = "is_skipped")
    private Boolean isSkipped;

    @Column(name = "skip_reason")
    private String skipReason;

    // === Getters & Setters ===

    public Integer getId()                  { return id; }
    public void    setId(Integer id)        { this.id = id; }

    public String  getRunId()               { return runId; }
    public void    setRunId(String runId)   { this.runId = runId; }

    public Integer getIterationId()                     { return iterationId; }
    public void    setIterationId(Integer iterationId)  { this.iterationId = iterationId; }

    public LocalDateTime getExecutionDate()                         { return executionDate; }
    public void          setExecutionDate(LocalDateTime executionDate) { this.executionDate = executionDate; }

    public String  getTableName()                   { return tableName; }
    public void    setTableName(String tableName)   { this.tableName = tableName; }

    public String  getColumnName()                    { return columnName; }
    public void    setColumnName(String columnName)   { this.columnName = columnName; }

    public String  getRule()                { return rule; }
    public void    setRule(String rule)     { this.rule = rule; }

    public String  getRuleDescription()                       { return ruleDescription; }
    public void    setRuleDescription(String ruleDescription) { this.ruleDescription = ruleDescription; }

    public String  getErrorCategory()                       { return errorCategory; }
    public void    setErrorCategory(String errorCategory)   { this.errorCategory = errorCategory; }

    public Integer getCountSource()                     { return countSource; }
    public void    setCountSource(Integer countSource)  { this.countSource = countSource; }

    public Integer getNbViolations()                      { return nbViolations; }
    public void    setNbViolations(Integer nbViolations)  { this.nbViolations = nbViolations; }

    public Integer getNbToCorrect()                     { return nbToCorrect; }
    public void    setNbToCorrect(Integer nbToCorrect)  { this.nbToCorrect = nbToCorrect; }

    public Integer getNbToMigrate()                     { return nbToMigrate; }
    public void    setNbToMigrate(Integer nbToMigrate)  { this.nbToMigrate = nbToMigrate; }

    public BigDecimal getTauxRejet()                       { return tauxRejet; }
    public void       setTauxRejet(BigDecimal tauxRejet)  { this.tauxRejet = tauxRejet; }
    public Integer getNbToCorrectAfter()                        { return nbToCorrectAfter; }
    public void    setNbToCorrectAfter(Integer nbToCorrectAfter){ this.nbToCorrectAfter = nbToCorrectAfter; }

    public Integer getNbViolationsAfter()                         { return nbViolationsAfter; }
    public void    setNbViolationsAfter(Integer nbViolationsAfter){ this.nbViolationsAfter = nbViolationsAfter; }

    public BigDecimal getTauxRejetAfter()                        { return tauxRejetAfter; }
    public void       setTauxRejetAfter(BigDecimal tauxRejetAfter){ this.tauxRejetAfter = tauxRejetAfter; }
    public Integer getNbToMigrateAfter()                          { return nbToMigrateAfter; }
    public void    setNbToMigrateAfter(Integer nbToMigrateAfter) { this.nbToMigrateAfter = nbToMigrateAfter; }

    public Boolean getIsSkipped()                   { return isSkipped; }
    public void    setIsSkipped(Boolean isSkipped)  { this.isSkipped = isSkipped; }

    public String  getSkipReason()                    { return skipReason; }
    public void    setSkipReason(String skipReason)   { this.skipReason = skipReason; }
}