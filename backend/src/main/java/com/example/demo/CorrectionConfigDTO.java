package com.example.demo;
import java.util.List;
import java.util.Map;

import lombok.Data;
import java.util.List;

@Data
public class CorrectionConfigDTO {
    private List<TableSelectionDTO> selectedTables;
    private String createdBy;
    private String detectionRunId;
    private List<Map<String, Object>> ruleBindings;
}

@Data
class TableSelectionDTO {
    private List<Long> selectedRuleIds;
}