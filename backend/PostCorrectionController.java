package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/post-correction")
@CrossOrigin(origins = "http://localhost:5173", allowCredentials = "true")
public class PostCorrectionController {

    @Autowired
    private PostCorrectionService postCorrectionService;


    @PostMapping("/launch-etl")
    public ResponseEntity<Map<String, Object>> launchEtlWithDetectionRunId(
            @RequestBody Map<String, Object> body) {
        try {
            Integer iterationId    = (Integer) body.get("iterationId");
            String  detectionRunId = (String)  body.get("detectionRunId");
            String  triggeredBy    = (String)  body.getOrDefault("triggeredBy", "unknown");

            Map<String, Object> result = postCorrectionService
                    .launchEtlDagWithDetectionRunId(iterationId, detectionRunId, triggeredBy);
            return ResponseEntity.ok(result);

        } catch (Exception e) {
            Map<String, Object> error = new HashMap<>();
            error.put("success", false);
            error.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(error);
        }
    }

    /**
     * Résultats avant/après pour un run_id de détection
     * GET /api/post-correction/results/{detectionRunId}
     */
    @GetMapping("/results/{detectionRunId}")
    public ResponseEntity<List<Map<String, Object>>> getResultsByRunId(
            @PathVariable String detectionRunId) {
        List<Map<String, Object>> results = postCorrectionService
                .getBeforeAfterResults(detectionRunId);
        return ResponseEntity.ok(results);
    }

    /**
     * Résumé par table (taux d'amélioration)
     * GET /api/post-correction/summary/{detectionRunId}
     */
    @GetMapping("/summary/{detectionRunId}")
    public ResponseEntity<List<Map<String, Object>>> getSummaryByTable(
            @PathVariable String detectionRunId) {
        List<Map<String, Object>> summary = postCorrectionService
                .getTableSummary(detectionRunId);
        return ResponseEntity.ok(summary);
    }

    /**
     * Tous les runs de détection qui ont un avant_correction rempli
     * GET /api/post-correction/available-runs
     */
    @GetMapping("/available-runs")
    public ResponseEntity<List<Map<String, Object>>> getAvailableRuns() {
        List<Map<String, Object>> runs = postCorrectionService.getAvailableDetectionRuns();
        return ResponseEntity.ok(runs);
    }
}