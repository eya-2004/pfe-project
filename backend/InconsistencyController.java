package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/inconsistencies")
@CrossOrigin(origins = "http://localhost:5173", allowCredentials = "true")
public class InconsistencyController {

    @Autowired
    private InconsistencyRepository inconsistencyRepository;

    @Autowired
    private QualityRunRepository qualityRunRepository;

    @GetMapping
    public List<Inconsistency> getAllInconsistencies() {
        return inconsistencyRepository.findAll();
    }
    @GetMapping("/iteration/{iterationId}")
    public List<Inconsistency> getByIterationId(@PathVariable Integer iterationId) {
        return inconsistencyRepository.findByIterationId(iterationId);
    }

    @GetMapping("/last")
    public ResponseEntity<List<Inconsistency>> getLastIteration() {
        List<Integer> iterIds = inconsistencyRepository.findAllIterationIds();
        if (iterIds.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(inconsistencyRepository.findByIterationId(iterIds.get(0)));
    }

    @GetMapping("/tables")
    public List<String> getTables(@RequestParam(required = false) Integer iterationId) {
        if (iterationId == null) {
            List<Integer> iterIds = inconsistencyRepository.findAllIterationIds();
            if (iterIds.isEmpty()) return List.of();
            iterationId = iterIds.get(0);
        }
        return inconsistencyRepository.findDistinctTableNamesByIterationId(iterationId);
    }
    @GetMapping("/categories")
    public List<String> getCategories(@RequestParam(required = false) Integer iterationId) {
        if (iterationId == null) {
            List<Integer> iterIds = inconsistencyRepository.findAllIterationIds();
            if (iterIds.isEmpty()) return List.of();
            iterationId = iterIds.get(0);
        }
        return inconsistencyRepository.findDistinctErrorCategoriesByIterationId(iterationId);
    }
    @GetMapping("/columns")
    public List<String> getColumns(
            @RequestParam String tableName,
            @RequestParam(required = false) Integer iterationId) {
        if (iterationId == null) {
            List<Integer> iterIds = inconsistencyRepository.findAllIterationIds();
            if (iterIds.isEmpty()) return List.of();
            iterationId = iterIds.get(0);
        }
        return inconsistencyRepository.findDistinctColumnsByTableAndIteration(iterationId, tableName);
    }


    @GetMapping("/filter")
    public List<Inconsistency> getFiltered(
            @RequestParam(required = false) Integer iterationId,
            @RequestParam(required = false) String tableName,
            @RequestParam(required = false) String columnName,
            @RequestParam(required = false) String errorCategory,
            @RequestParam(defaultValue = "false") boolean onlyErrors) {

        if (iterationId == null) {
            List<Integer> iterIds = inconsistencyRepository.findAllIterationIds();
            if (iterIds.isEmpty()) return List.of();
            iterationId = iterIds.get(0);
        }

        List<Inconsistency> results;

        if (tableName != null && columnName != null) {
            results = inconsistencyRepository.findByIterationIdAndTableNameAndColumnName(
                    iterationId, tableName, columnName);
        } else if (tableName != null) {
            results = inconsistencyRepository.findByIterationIdAndTableName(iterationId, tableName);
        } else {
            results = inconsistencyRepository.findByIterationId(iterationId);
        }

        if (errorCategory != null) {
            results = results.stream()
                    .filter(r -> errorCategory.equals(r.getErrorCategory()))
                    .collect(Collectors.toList());
        }

        if (onlyErrors) {
            results = results.stream()
                    .filter(r -> r.getNbViolations() != null && r.getNbViolations() > 0)
                    .collect(Collectors.toList());
        }

        return results;
    }
    @GetMapping("/iteration/{iterationId}/run/{runId}")
    public ResponseEntity<Map<String, Object>> getByIterationAndRun(
            @PathVariable Integer iterationId,
            @PathVariable String runId) {

        Map<String, Object> response = new java.util.LinkedHashMap<>();
        try {
            List<Inconsistency> data = inconsistencyRepository
                    .findByIterationIdAndRunId(iterationId, runId);

            response.put("found", !data.isEmpty());
            response.put("iterationId", iterationId);
            response.put("runId", runId);
            response.put("data", data);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            response.put("found", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    @GetMapping("/{id}")
    public ResponseEntity<Inconsistency> getById(@PathVariable Integer id) {
        return inconsistencyRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}