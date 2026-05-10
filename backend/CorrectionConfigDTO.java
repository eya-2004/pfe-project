package com.example.demo;
import java.util.List;
import java.util.Map;

import lombok.Data;
import java.util.List;

@Data
public class CorrectionConfigDTO {
    private Integer iterationId;
    private List<TableSelectionDTO> selectedTables;
    private List<RuleSelectionDTO> selectedRules;
    private String createdBy;
    private String detectionRunId;
    private List<Map<String, Object>> ruleBindings;
}

@Data
class RuleSelectionDTO {
    private Long ruleId;
    private Boolean toExecute;

}
@Data
class TableSelectionDTO {
    private String tableName;
    private Integer batchSize;
    private List<Long> selectedRuleIds;
    private Boolean toExecute;
}