package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.springframework.jdbc.core.JdbcTemplate;
@RestController
@RequestMapping("/api/correction-config")
@CrossOrigin(origins = "http://localhost:5173")
public class CorrectionConfigController {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private CorrectionConfigService correctionConfigService;
    @GetMapping("/next-iteration-id")
    public ResponseEntity<Integer> getNextIterationId() {
        Integer nextId = correctionConfigService.getNextIterationId();
        return ResponseEntity.ok(nextId);
    }
    @GetMapping("/iterations")  // ✅ NOUVEAU : Historique des itérations
    public ResponseEntity<List<Map<String, Object>>> getAllIterations() {
        List<Map<String, Object>> iterations = correctionConfigService.getAllIterations();
        return ResponseEntity.ok(iterations);
    }
    @GetMapping("/tables")
    public ResponseEntity<List<TableGroupDTO>> getAllTablesWithRules() {
        // ✅ CORRIGÉ: Utilisez le BON nom de méthode du Service
        List<TableGroupDTO> tables = correctionConfigService.getAllTablesWithRealRowCount();
        return ResponseEntity.ok(tables);
    }

    @PostMapping("/save")
    public ResponseEntity<Map<String, Object>> saveCorrectionConfig(
            @RequestBody CorrectionConfigDTO configDTO) {
        try {
            Map<String, Object> result = correctionConfigService.saveCorrectionConfig(configDTO);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", "Erreur lors de l'enregistrement : " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }
    @GetMapping("/run-errors/{runId}")
    public ResponseEntity<List<Map<String, Object>>> getRunErrors(@PathVariable String runId) {
        String sql = """
        SELECT table_name as tableName, column_name as columnName , rule_label as ruleLabel,
               COUNT(DISTINCT row_pk_value) as nbViolations
        FROM bscs_problematic_rows
        WHERE run_id = ?
        GROUP BY table_name, column_name, rule_label
        ORDER BY table_name, column_name, rule_label
    """;
        return ResponseEntity.ok(jdbcTemplate.queryForList(sql,runId));
    }
    @GetMapping("/iteration/{dagRunId}")
    public ResponseEntity<?> getIterationByDagRunId(@PathVariable String dagRunId) {
        return ResponseEntity.ok().build();
    }
    @GetMapping("/transformation-rules")
    public ResponseEntity<List<Map<String, Object>>> getTransformationRules(
            @RequestParam String tableName,
            @RequestParam String columnName) {
        String sql = """
        SELECT rule_id as ruleId, rule_type as ruleType,
               rule_label as ruleLabel, default_value as defaultValue
        FROM bss_migration_rules
        WHERE LOWER(source_table)  = LOWER(?)
          AND LOWER(source_column) = LOWER(?)
        ORDER BY rule_id
    """;
        return ResponseEntity.ok(jdbcTemplate.queryForList(sql, tableName, columnName));
    }
    @GetMapping("/available-runs")
    public ResponseEntity<List<Map<String, Object>>> getAvailableDetectionRuns() {
        List<Map<String, Object>> runs = correctionConfigService.getAvailableDetectionRuns();
        return ResponseEntity.ok(runs);
    }

}