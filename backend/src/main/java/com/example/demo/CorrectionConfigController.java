package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/correction-config")
@CrossOrigin(origins = "http://localhost:5173")
public class CorrectionConfigController {

    @Autowired
    private CorrectionConfigService correctionConfigService;
    @GetMapping("/next-iteration-id")  // ✅ NOUVEAU : Pour avoir le prochain ID avant de sauvegarder
    public ResponseEntity<Integer> getNextIterationId() {
        Integer nextId = correctionConfigService.getNextIterationId();
        return ResponseEntity.ok(nextId);
    }
    @GetMapping("/last-saved-iteration-id")
    public ResponseEntity<Integer> getLastSavedIterationId() {
        Integer lastId = correctionConfigService.getLastSavedIterationId();
        return ResponseEntity.ok(lastId);
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
        Map<String, Object> result = correctionConfigService.saveCorrectionConfig(configDTO);
        return ResponseEntity.ok(result);
    }

    @GetMapping("/iteration/{dagRunId}")
    public ResponseEntity<?> getIterationByDagRunId(@PathVariable String dagRunId) {
        return ResponseEntity.ok().build();
    }
    @PostMapping("/launch-airflow/{iterationId}")
    public ResponseEntity<Map<String, Object>> launchAirflowDag(
            @PathVariable Integer iterationId,
            @RequestBody(required = false) Map<String, String> body) {

        String triggeredBy = (body != null) ? body.getOrDefault("triggeredBy", "unknown") : "unknown";
        Map<String, Object> result = correctionConfigService.launchAirflowDag(iterationId, triggeredBy);
        return ResponseEntity.ok(result);
    }
}