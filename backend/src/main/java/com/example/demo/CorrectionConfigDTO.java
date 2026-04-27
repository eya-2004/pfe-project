package com.example.demo;



import lombok.Data;
import java.util.List;

@Data
public class CorrectionConfigDTO {
    private Integer iterationId;
    private List<TableSelectionDTO> selectedTables;
    private List<RuleSelectionDTO> selectedRules;
    private String createdBy;
}

@Data
class RuleSelectionDTO {
    private Long ruleId;
    private Boolean toExecute;
    private Integer rowLimit;
    private Integer offsetCurrent;
}
@Data
class TableSelectionDTO {
    private String tableName;
    private Integer batchSize;
    private List<Long> selectedRuleIds;
    private Boolean toExecute;
}