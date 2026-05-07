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

    @Transactional
    public Map<String, Object> saveCorrectionConfig(CorrectionConfigDTO configDTO) {
        String dagRunId = generateDagRunId();
        Integer nextIterationId = getNextIterationId();

        List<MigrationIterationConfig> configs = new ArrayList<>();

        List<TableSelectionDTO> tablesToProcess = configDTO.getSelectedTables();

        // Récupérer les ruleBindings s'ils existent
        List<Map<String, Object>> ruleBindings = configDTO.getRuleBindings();

        // Créer un map pour retrouver facilement les infos d'un ruleId
        Map<Long, Map<String, Object>> ruleInfoMap = new HashMap<>();
        if (ruleBindings != null) {
            for (Map<String, Object> binding : ruleBindings) {
                Long ruleId = ((Number) binding.get("transformationRuleId")).longValue();
                ruleInfoMap.put(ruleId, binding);
            }
        }

        if (tablesToProcess == null || tablesToProcess.isEmpty()) {
            // Cas par défaut - créer une config simple
            MigrationIterationConfig defaultConfig = new MigrationIterationConfig();
            defaultConfig.setIterationId(nextIterationId);
            defaultConfig.setToExecute(MigrationIterationConfig.ToExecuteStatus.YES);

            defaultConfig.setCreatedBy(configDTO.getCreatedBy());
            defaultConfig.setDagRunId(dagRunId);
            defaultConfig.setCreatedAt(LocalDateTime.now());
            defaultConfig.setDetectionRunId(configDTO.getDetectionRunId());

            // ✅ Si on a des bindings, utiliser le premier pour remplir les champs
            if (ruleBindings != null && !ruleBindings.isEmpty()) {
                Map<String, Object> firstBinding = ruleBindings.get(0);
                defaultConfig.setDetectionRuleLabel((String) firstBinding.get("detectionRuleLabel"));
                defaultConfig.setTargetColumn((String) firstBinding.get("targetColumn"));
            }

            configs.add(defaultConfig);

        } else {
            // Cas normal avec selectedTables
            for (TableSelectionDTO tableSelection : tablesToProcess) {

                if (tableSelection.getSelectedRuleIds() != null && !tableSelection.getSelectedRuleIds().isEmpty()) {

                    for (int i = 0; i < tableSelection.getSelectedRuleIds().size(); i++) {
                        MigrationIterationConfig config = new MigrationIterationConfig();

                        Long currentRuleId = tableSelection.getSelectedRuleIds().get(i);

                        config.setIterationId(nextIterationId);
                        config.setRuleId(currentRuleId);
                        config.setToExecute(tableSelection.getToExecute() != null && tableSelection.getToExecute() ?
                                MigrationIterationConfig.ToExecuteStatus.YES :
                                MigrationIterationConfig.ToExecuteStatus.NO);


                        config.setCreatedBy(configDTO.getCreatedBy());
                        config.setDagRunId(dagRunId);
                        config.setCreatedAt(LocalDateTime.now());
                        config.setDetectionRunId(configDTO.getDetectionRunId());

                        // ✅ REMPLIR LES CHAMPS MANQUANTS depuis ruleInfoMap
                        if (ruleInfoMap.containsKey(currentRuleId)) {
                            Map<String, Object> info = ruleInfoMap.get(currentRuleId);
                            config.setDetectionRuleLabel((String) info.get("detectionRuleLabel"));
                            config.setTargetColumn((String) info.get("targetColumn"));
                        } else {
                            // Fallback : chercher dans tous les bindings pour cette table
                            if (ruleBindings != null) {
                                for (Map<String, Object> binding : ruleBindings) {
                                    if (((Number) binding.get("transformationRuleId")).longValue() == currentRuleId) {
                                        config.setDetectionRuleLabel((String) binding.get("detectionRuleLabel"));
                                        config.setTargetColumn((String) binding.get("targetColumn"));
                                        break;
                                    }
                                }
                            }
                        }

                        configs.add(config);
                    }

                } else {
                    // Pas de règles sélectionnées pour cette table
                    MigrationIterationConfig config = new MigrationIterationConfig();
                    config.setIterationId(nextIterationId);
                    config.setToExecute(MigrationIterationConfig.ToExecuteStatus.YES);

                    config.setCreatedBy(configDTO.getCreatedBy());
                    config.setDagRunId(dagRunId);
                    config.setCreatedAt(LocalDateTime.now());
                    config.setDetectionRunId(configDTO.getDetectionRunId());
                    configs.add(config);
                }
            }
        }

        configRepository.saveAll(configs);

        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("dagRunId", dagRunId);
        response.put("iterationId", nextIterationId);
        response.put("totalTablesSaved", tablesToProcess != null ? tablesToProcess.size() : 1);
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
    public List<Map<String, Object>> getAvailableDetectionRuns() {
        String sql = """
        SELECT DISTINCT run_id, execution_date
        FROM bscs_detected_inconsistency
        ORDER BY execution_date DESC
    """;
        return jdbcTemplate.queryForList(sql);
    }

    private String generateDagRunId() {
        return "CORR_" + UUID.randomUUID().toString().substring(0, 8).toUpperCase() +
                "_" + LocalDateTime.now().toString().replace(":", "-");
    }
}