package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import java.time.Instant;
@Service
public class CorrectionConfigService {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MigrationIterationConfigRepository configRepository;

    @Value("${airflow.api.url}")
    private String airflowBaseUrl;

    @Value("${airflow.dag.correction-id:bss_migration_etl}")
    private String correctionDagId;

    @Value("${airflow.api.username}")
    private String airflowUsername;

    @Value("${airflow.api.password}")
    private String airflowPassword;

    @Autowired
    private BssMigrationRuleRepository ruleRepository;

    public Integer getNextIterationId() {
        try {
            String sql = "SELECT COALESCE(MAX(iteration_id), 0) FROM correction_iteration_config";
            Integer maxId = jdbcTemplate.queryForObject(sql, Integer.class);
            return (maxId != null ? maxId : 0) + 1;
        } catch (Exception e) {
            System.err.println("Erreur récupération next iteration id: " + e.getMessage());
            return 1; // Par défaut si table vide ou erreur
        }
    }
    @Transactional
    public Map<String, Object> saveCorrectionConfig(CorrectionConfigDTO configDTO) {
        String dagRunId = generateDagRunId();
        Integer nextIterationId = getNextIterationId();

        List<MigrationIterationConfig> configs = new ArrayList<>();
        List<TableSelectionDTO> tablesToProcess = configDTO.getSelectedTables();
        List<Map<String, Object>> ruleBindings = configDTO.getRuleBindings();

        // Construire le map ruleId → binding
        Map<Long, Map<String, Object>> ruleInfoMap = new HashMap<>();
        for (Map<String, Object> binding : ruleBindings) {
            Long ruleId = ((Number) binding.get("transformationRuleId")).longValue();
            ruleInfoMap.put(ruleId, binding);
        }

        // Une config par ruleId par table
        for (TableSelectionDTO tableSelection : tablesToProcess) {
            for (Long currentRuleId : tableSelection.getSelectedRuleIds()) {
                MigrationIterationConfig config = new MigrationIterationConfig();
                config.setIterationId(nextIterationId);
                config.setRuleId(currentRuleId);
                config.setToExecute(MigrationIterationConfig.ToExecuteStatus.YES);
                config.setCreatedBy(configDTO.getCreatedBy());
                config.setDagRunId(dagRunId);
                config.setCreatedAt(LocalDateTime.now());
                config.setDetectionRunId(configDTO.getDetectionRunId());

                Map<String, Object> info = ruleInfoMap.get(currentRuleId);
                if (info != null) {
                    config.setDetectionRuleLabel((String) info.get("detectionRuleLabel"));
                    config.setTargetColumn((String) info.get("targetColumn"));
                }

                configs.add(config);
            }
        }

        configRepository.saveAll(configs);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("dagRunId", dagRunId);
        response.put("iterationId", nextIterationId);
        response.put("totalTablesSaved", tablesToProcess.size());
        response.put("totalRulesSaved", configs.size());
        response.put("message", "Itération #" + nextIterationId + " sauvegardée avec succès");

        return response;
    }

    public List<BssMigrationRule> getTransformationRules(String tableName, String columnName) {
        return ruleRepository.findBySourceTableIgnoreCaseAndSourceColumnIgnoreCase(
            tableName, columnName
        );
    }
    
    public Map<String, Object> launchEtlDagWithDetectionRunId(
            Integer iterationId, String detectionRunId, String triggeredBy) {

        List<MigrationIterationConfig> configs = configRepository.findByIterationId(iterationId);
        if (configs.isEmpty()) {
            throw new RuntimeException("Aucune configuration trouvée pour l'itération #" + iterationId);
        }

        String dagRunId = "ETL_" + iterationId + "_" + Instant.now().getEpochSecond();
        RestTemplate restTemplate = new RestTemplate();
        String accessToken;

        // 2. Récupération du token JWT
        try {
            String tokenUrl = airflowBaseUrl + "/auth/token";
            HttpHeaders tokenHeaders = new HttpHeaders();
            tokenHeaders.setContentType(MediaType.APPLICATION_JSON);
            Map<String, String> credentials = Map.of(
                    "username", airflowUsername,
                    "password", airflowPassword);
            HttpEntity<Map<String, String>> tokenRequest = new HttpEntity<>(credentials, tokenHeaders);
            ResponseEntity<Map> tokenResponse = restTemplate.postForEntity(tokenUrl, tokenRequest, Map.class);

            if (tokenResponse.getBody() == null || !tokenResponse.getBody().containsKey("access_token")) {
                throw new RuntimeException("Réponse token invalide — access_token absent");
            }

            accessToken = (String) tokenResponse.getBody().get("access_token");
            System.out.println("==> Token obtenu: OUI");

        } catch (Exception e) {
            System.err.println("❌ Erreur authentification Airflow: " + e.getMessage());
            throw new RuntimeException("Échec de l'authentification Airflow : " + e.getMessage());
        }

        // 3. Appel Airflow
        try {
            Map<String, Object> conf = new HashMap<>();
            conf.put("iteration_id",     iterationId);
            conf.put("detection_run_id", detectionRunId);
            conf.put("triggered_by",     triggeredBy);

            Map<String, Object> payload = new HashMap<>();
            payload.put("dag_run_id",   dagRunId);
            payload.put("logical_date", Instant.now().toString());
            payload.put("conf",         conf);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(accessToken);

            String url = airflowBaseUrl + "/api/v2/dags/" + correctionDagId + "/dagRuns";
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);
            ResponseEntity<Map> airflowResponse = restTemplate.postForEntity(url, request, Map.class);
            try {
                jdbcTemplate.update(
                        "UPDATE correction_iteration_config SET detection_run_id = ?, dag_run_id = ? WHERE iteration_id = ?",
                        detectionRunId, dagRunId, iterationId
                );
            } catch (Exception e) {
                System.err.println("❌ Erreur mise à jour base : " + e.getMessage());
            }

            Map<String, Object> result = new HashMap<>();
            result.put("iterationId",    iterationId);
            result.put("detectionRunId", detectionRunId);
            result.put("dagRunId",       dagRunId);
            result.put("triggeredBy",    triggeredBy);
            result.put("status",         airflowResponse.getBody() != null
                    ? airflowResponse.getBody().get("state")
                    : "triggered");
            return result;

        } catch (Exception e) {
            System.err.println("❌ Erreur lancement DAG Airflow: " + e.getMessage());
            throw new RuntimeException("Échec du lancement du DAG Airflow : " + e.getMessage());
        }
    }
    private String generateDagRunId() {
    return "CORR_" + UUID.randomUUID().toString().substring(0, 8).toUpperCase() +
            "_" + LocalDateTime.now().toString().replace(":", "-");
}
public List<Map<String, Object>> getAvailableDetectionRuns() {
    String sql = """
        SELECT DISTINCT run_id, execution_date
        FROM bscs_detected_inconsistency
        ORDER BY execution_date DESC
    """;
    return jdbcTemplate.queryForList(sql);
}

}