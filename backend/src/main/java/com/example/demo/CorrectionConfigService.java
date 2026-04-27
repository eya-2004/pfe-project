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
    private BssMigrationRuleRepository ruleRepository;

    @Autowired
    private MigrationIterationConfigRepository configRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;


    public List<TableGroupDTO> getAllTablesWithRealRowCount() {
        List<String> tables = ruleRepository.findDistinctSourceTables();

        return tables.stream().map(tableName -> {
            TableGroupDTO dto = new TableGroupDTO();
            dto.setTableName(tableName);

            long realRowCount = countRowsInTable(tableName);
            dto.setTotalRows(realRowCount);

            dto.setBatchSize(null); // null = pas encore configuré

            List<BssMigrationRule> rules =
                    ruleRepository.findBySourceTableOrderBySourceColumn(tableName);

            dto.setRules(rules.stream().map(rule -> {
                RuleInfoDTO ruleDto = new RuleInfoDTO();
                ruleDto.setRuleId(rule.getRuleId());
                ruleDto.setSourceColumn(rule.getSourceColumn());
                ruleDto.setRuleLabel(rule.getRuleLabel());
                ruleDto.setRuleType(rule.getRuleType());
                ruleDto.setDefaultValue(rule.getDefaultValue());
                return ruleDto;
            }).collect(Collectors.toList()));

            return dto;
        }).collect(Collectors.toList());
    }


    public Integer getNextIterationId() {
        try {
            // Chercher le max iteration_id existant dans la table
            String sql = "SELECT COALESCE(MAX(iteration_id), 0) FROM correction_iteration_config";
            Integer maxId = jdbcTemplate.queryForObject(sql, Integer.class);
            return (maxId != null ? maxId : 0) + 1; // Incrémenter de 1
        } catch (Exception e) {
            System.err.println("Erreur récupération next iteration id: " + e.getMessage());
            return 1; // Par défaut si table vide ou erreur
        }
    }

    private long countRowsInTable(String tableName) {
        try {
            if (!isValidTableName(tableName)) {
                return 0;
            }

            String sql = "SELECT COUNT(*) FROM " + tableName;
            Long count = jdbcTemplate.queryForObject(sql, Long.class);
            return count != null ? count : 0L;
        } catch (Exception e) {
            System.err.println("Erreur comptage table " + tableName + ": " + e.getMessage());
            return 0L;
        }
    }

    private boolean isValidTableName(String tableName) {
        List<String> allowedTables = Arrays.asList(
                "bscs_billing_account",
                "bscs_billing_account_assign",
                "bscs_charge",
                "bscs_customer",
                "bscs_findocs",
                "bscs_payments",
                "bscs_payments_det",
                "bscs_place",
                "bscs_resource_directory",
                "bscs_resource_port",
                "bscs_resource_sim",
                "bscs_services"
        );
        return allowedTables.contains(tableName.toLowerCase());
    }
    public Integer getLastSavedIterationId() {
        return jdbcTemplate.queryForObject(
                "SELECT MAX(iteration_id) FROM  correction_iteration_config ", Integer.class
        );

    }

    @Transactional
    public Map<String, Object> saveCorrectionConfig(CorrectionConfigDTO configDTO) {
        String dagRunId = generateDagRunId();

        Integer nextIterationId = getNextIterationId();

        List<MigrationIterationConfig> configs = new ArrayList<>();

        for (TableSelectionDTO tableSelection : configDTO.getSelectedTables()) {
            MigrationIterationConfig config = new MigrationIterationConfig();

            config.setIterationId(nextIterationId);

            config.setToExecute(tableSelection.getToExecute() != null && tableSelection.getToExecute() ?
                    MigrationIterationConfig.ToExecuteStatus.YES :
                    MigrationIterationConfig.ToExecuteStatus.NO);

            if (tableSelection.getBatchSize() != null && tableSelection.getBatchSize() > 0) {
                config.setRowLimit(tableSelection.getBatchSize());
            } else {
                config.setRowLimit(null);
            }

            config.setOffsetCurrent(0);
            config.setCreatedBy(configDTO.getCreatedBy());
            config.setDagRunId(dagRunId);
            config.setCreatedAt(LocalDateTime.now());

            if (tableSelection.getSelectedRuleIds() != null &&
                    !tableSelection.getSelectedRuleIds().isEmpty()) {

                config.setRuleId(tableSelection.getSelectedRuleIds().get(0));

                // Autres règles
                for (int i = 1; i < tableSelection.getSelectedRuleIds().size(); i++) {
                    MigrationIterationConfig additionalConfig = new MigrationIterationConfig();
                    additionalConfig.setIterationId(nextIterationId); // ✅ Même ID
                    additionalConfig.setRuleId(tableSelection.getSelectedRuleIds().get(i));
                    additionalConfig.setToExecute(config.getToExecute());
                    additionalConfig.setRowLimit(config.getRowLimit()); // ✅ Même batch size
                    additionalConfig.setOffsetCurrent(0);
                    additionalConfig.setCreatedBy(config.getCreatedBy());
                    additionalConfig.setDagRunId(dagRunId);
                    additionalConfig.setCreatedAt(LocalDateTime.now());
                    configs.add(additionalConfig);
                }
            }

            configs.add(config);
        }

        configRepository.saveAll(configs);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("dagRunId", dagRunId);
        response.put("iterationId", nextIterationId); // ✅ Retourner le VRAI ID
        response.put("totalTablesSaved", configDTO.getSelectedTables().size());
        response.put("totalRulesSaved", configs.size());
        response.put("message", "Itération #" + nextIterationId + " sauvegardée avec succès");

        return response;
    }


    public List<Map<String, Object>> getAllIterations() {
        String sql = "SELECT iteration_id, dag_run_id, created_by, COUNT(*) as total_rules, " +
                "MAX(created_at) as created_at, SUM(CASE WHEN to_execute='YES' THEN 1 ELSE 0 END) as active_rules " +
                "FROM correction_iteration_config " +
                "GROUP BY iteration_id, dag_run_id, created_by " +
                "ORDER BY iteration_id DESC";

        return jdbcTemplate.queryForList(sql);
    }

    @Value("${airflow.api.url}")
    private String airflowBaseUrl;

    @Value("${airflow.dag.correction-id:bss_migration_etl}")  // valeur par défaut si absent
    private String airflowDagId;

    @Value("${airflow.api.username}")
    private String airflowUsername;

    @Value("${airflow.api.password}")
    private String airflowPassword;

    public Map<String, Object> launchAirflowDag(Integer iterationId, String triggeredBy) {
        List<MigrationIterationConfig> configs = configRepository.findByIterationId(iterationId);
        if (configs.isEmpty()) {
            throw new RuntimeException("Aucune configuration trouvée pour l'itération #" + iterationId);
        }

        String dagRunId = "iteration_" + iterationId + "_" + Instant.now().getEpochSecond();
        RestTemplate restTemplate = new RestTemplate();

        // 1. Récupérer le token JWT
        String tokenUrl = airflowBaseUrl + "/auth/token";
        HttpHeaders tokenHeaders = new HttpHeaders();
        tokenHeaders.setContentType(MediaType.APPLICATION_JSON);
        Map<String, String> credentials = Map.of("username", airflowUsername, "password", airflowPassword);
        HttpEntity<Map<String, String>> tokenRequest = new HttpEntity<>(credentials, tokenHeaders);
        ResponseEntity<Map> tokenResponse = restTemplate.postForEntity(tokenUrl, tokenRequest, Map.class);
        String accessToken = (String) tokenResponse.getBody().get("access_token");
        System.out.println("==> Token obtenu: " + (accessToken != null ? "OUI" : "NON"));

        // 2. Appeler Airflow avec le token
        Map<String, Object> airflowPayload = new HashMap<>();
        airflowPayload.put("dag_run_id", dagRunId);
        airflowPayload.put("logical_date", Instant.now().toString()); // ✅ Requis par Airflow 3.x
        airflowPayload.put("conf", Map.of("iteration_id", iterationId, "triggered_by", triggeredBy));

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(accessToken);

        String airflowUrl = airflowBaseUrl + "/api/v2/dags/" + airflowDagId + "/dagRuns";
        System.out.println("==> URL: " + airflowUrl);

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(airflowPayload, headers);
        ResponseEntity<Map> airflowResponse = restTemplate.postForEntity(airflowUrl, request, Map.class);

        // 3. Mettre à jour en base
        configs.forEach(config -> {
            config.setDagRunId(dagRunId);
            configRepository.save(config);
        });

        // 4. Retourner la réponse
        Map<String, Object> result = new HashMap<>();
        result.put("iterationId", iterationId);
        result.put("dagRunId", dagRunId);
        result.put("dagId", airflowDagId);
        result.put("triggeredBy", triggeredBy);
        result.put("status", airflowResponse.getBody() != null ? airflowResponse.getBody().get("state") : "triggered");
        result.put("airflowResponse", airflowResponse.getBody());

        return result;
    }
    private String generateDagRunId() {
        return "CORR_" + UUID.randomUUID().toString().substring(0, 8).toUpperCase() +
                "_" + LocalDateTime.now().toString().replace(":", "-");
    }
}