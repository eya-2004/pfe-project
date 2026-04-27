package com.example.demo;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "bscs_error_details") // Mappe à la table existante
public class BscsErrorDetail {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(name = "source_run_id")
    private String sourceRunId;
    
    @Column(name = "table_name")
    private String tableName;
    
    @Column(name = "row_pk_value")
    private String rowPkValue;
    
    @Column(name = "full_row_data")
    private String fullRowData;
    
    @Column(name = "error_categories")
    private String errorCategories;
    
    @Column(name = "column_errors")
    private String columnErrors;
    
    @Column(name = "export_date")
    private LocalDateTime exportDate;
    
    @Column(name = "correction_status")
    private String correctionStatus;
    
    @Column(name = "corrected_data")
    private String correctedData;
    
    @Column(name = "corrected_by")
    private String correctedBy;
    
    @Column(name = "correction_comment")
    private String correctionComment;
    
    @Column(name = "correction_date")
    private LocalDateTime correctionDate;

    // Constructeurs, Getters et Setters...
    // (Je les laisse pour la complétude, ils sont nécessaires pour JPA)
    public BscsErrorDetail() {}

    // Getters et Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getSourceRunId() { return sourceRunId; }
    public void setSourceRunId(String sourceRunId) { this.sourceRunId = sourceRunId; }

    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }

    public String getRowPkValue() { return rowPkValue; }
    public void setRowPkValue(String rowPkValue) { this.rowPkValue = rowPkValue; }

    public String getFullRowData() { return fullRowData; }
    public void setFullRowData(String fullRowData) { this.fullRowData = fullRowData; }

    public String getErrorCategories() { return errorCategories; }
    public void setErrorCategories(String errorCategories) { this.errorCategories = errorCategories; }

    public String getColumnErrors() { return columnErrors; }
    public void setColumnErrors(String columnErrors) { this.columnErrors = columnErrors; }

    public LocalDateTime getExportDate() { return exportDate; }
    public void setExportDate(LocalDateTime exportDate) { this.exportDate = exportDate; }

    public String getCorrectionStatus() { return correctionStatus; }
    public void setCorrectionStatus(String correctionStatus) { this.correctionStatus = correctionStatus; }

    public String getCorrectedData() { return correctedData; }
    public void setCorrectedData(String correctedData) { this.correctedData = correctedData; }

    public String getCorrectedBy() { return correctedBy; }
    public void setCorrectedBy(String correctedBy) { this.correctedBy = correctedBy; }

    public String getCorrectionComment() { return correctionComment; }
    public void setCorrectionComment(String correctionComment) { this.correctionComment = correctionComment; }

    public LocalDateTime getCorrectionDate() { return correctionDate; }
    public void setCorrectionDate(LocalDateTime correctionDate) { this.correctionDate = correctionDate; }
}