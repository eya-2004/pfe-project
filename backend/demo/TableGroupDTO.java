package com.example.demo;


import lombok.Data;
import java.util.List;

@Data
public class TableGroupDTO {
    private String tableName;
    private Long totalRows;
    private Integer batchSize;
    private List<RuleInfoDTO> rules;
}

@Data
class RuleInfoDTO {
    private Long ruleId;
    private String ruleLabel;
    private String ruleType;
    private String sourceColumn;
    private String defaultValue;
}