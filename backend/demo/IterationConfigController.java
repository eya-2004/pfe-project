package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.Base64;

@RestController
@RequestMapping("/api/iterations")
@CrossOrigin(origins = "http://localhost:5173", allowCredentials = "true")
public class IterationConfigController {

    // ─── Injection ────────────────────────────────────────────
    @Autowired
    private InconsistencyRepository inconsistencyRepository;
    @Autowired
    private IterationConfigRepository iterationConfigRepository;

    @Autowired
    private DataSource dataSource;

    @Value("${airflow.api.url:http://localhost:8080}")
    private String airflowApiUrl;

    @Value("${airflow.api.username:admin}")
    private String airflowUser;

    @Value("${airflow.api.password:admin}")
    private String airflowPassword;

    @Value("${airflow.dag.id:detected_inconsistencies}")
    private String airflowDagId;

    @Value("${airflow.api.connect-timeout:10000}")
    private int airflowConnectTimeout;

    @Value("${airflow.api.read-timeout:30000}")
    private int airflowReadTimeout;

    private static final Logger log = Logger.getLogger(IterationConfigController.class.getName());

    /**
     * RestTemplate avec Apache HttpClient — obligatoire pour supporter PATCH.
     */
    private RestTemplate buildRestTemplate() {
        HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory();
        factory.setConnectTimeout(airflowConnectTimeout);
        factory.setConnectionRequestTimeout(airflowReadTimeout);
        return new RestTemplate(factory);
    }

    // ─── Tables exclues ────────────────────────────────────────

    private static final Set<String> EXCLUDED_TABLES = Set.of(
            "bscs_detected_inconsistency", "bscs_quality_run", "bscs_problematic_rows",
            "bscs_iteration_config", "bscs_error_rows", "bscs_migration_decision",
            "bscs_notifications", "bscs_payment_anomalies", "bscs_findocs_errors",
            "map_currency", "map_country", "dag_compare_rule_result"
    );

    private static final Set<String> BUSINESS_TABLES = Set.of(
            "bscs_billing_account", "bscs_billing_account_assign", "bscs_charge",
            "bscs_customer", "bscs_customer_tax_exempt", "bscs_findocs",
            "bscs_memos", "bscs_payments", "bscs_payments_det", "bscs_place",
            "bscs_portability_hist", "bscs_portability_in", "bscs_pre_activation",
            "bscs_resource_directory", "bscs_resource_port", "bscs_resource_sim",
            "bscs_services", "bscs_services_parameter", "bscs_souscription",
            "carry_over", "contract_history", "ixc_dunprocess", "ixc_payment_plan"
    );

    @GetMapping("/next-iteration-id")
    public ResponseEntity<Integer> getNextIterationId() {
        Integer maxId = iterationConfigRepository.findMaxIterationId().orElse(0);
        return ResponseEntity.ok(maxId + 1);
    }
    @GetMapping("/list")
    public ResponseEntity<Map<String, Object>> listIterations() {
        Map<String, Object> response = new java.util.LinkedHashMap<>();
        try {
            List<Integer> iterationIds = inconsistencyRepository.findAllIterationIds();

            List<Map<String, Object>> iterations = new java.util.ArrayList<>();
            for (Integer iterId : iterationIds) {
                List<Object[]> runs = inconsistencyRepository.findRunsByIterationId(iterId);
                List<Map<String, Object>> runList = new java.util.ArrayList<>();
                for (Object[] row : runs) {
                    Map<String, Object> run = new java.util.LinkedHashMap<>();
                    run.put("runId", row[0]);
                    run.put("executionDate", row[1]);
                    runList.add(run);
                }

                // 👇 Ajouter cette ligne pour récupérer hasCorrection
                Boolean hasCorrection = iterationConfigRepository
                        .findHasCorrectionByIterationId(iterId)
                        .orElse(false);

                Map<String, Object> iter = new java.util.LinkedHashMap<>();
                iter.put("iterationId", iterId);
                iter.put("runs", runList);
                iter.put("hasCorrection", hasCorrection); // 👇 Ajouter ce champ
                iterations.add(iter);
            }

            response.put("success", true);
            response.put("iterations", iterations);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    @GetMapping("/available-tables")
    public ResponseEntity<Map<String, Object>> getAvailableTables() {
        Map<String, Object> response = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection()) {

            String currentSchema;
            try (Statement st = conn.createStatement();
                 ResultSet rs = st.executeQuery("SELECT DATABASE()")) {
                rs.next();
                currentSchema = rs.getString(1);
            }

            String placeholders = BUSINESS_TABLES.stream()
                    .map(t -> "?").collect(Collectors.joining(", "));

            String sql = String.format("""
                SELECT TABLE_NAME, TABLE_ROWS, TABLE_COMMENT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                  AND TABLE_TYPE = 'BASE TABLE'
                  AND LOWER(TABLE_NAME) IN (%s)
                ORDER BY TABLE_NAME
            """, placeholders);

            List<Object> params = new ArrayList<>();
            params.add(currentSchema);
            params.addAll(BUSINESS_TABLES.stream()
                    .map(String::toLowerCase)
                    .collect(Collectors.toList()));

            List<Map<String, Object>> tables = new ArrayList<>();
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < params.size(); i++) {
                    ps.setObject(i + 1, params.get(i));
                }
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> tableInfo = new LinkedHashMap<>();
                        tableInfo.put("tableName",     rs.getString("TABLE_NAME"));
                        tableInfo.put("estimatedRows", rs.getLong("TABLE_ROWS"));
                        tableInfo.put("comment",       rs.getString("TABLE_COMMENT"));
                        tables.add(tableInfo);
                    }
                }
            }

            response.put("success",     true);
            response.put("schema",      currentSchema);
            response.put("tables",      tables);
            response.put("totalTables", tables.size());
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Erreur récupération tables: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }


    @GetMapping("/available-tables/{tableName}/rules")
    public ResponseEntity<Map<String, Object>> getRulesForTable(@PathVariable String tableName) {
        Map<String, Object> response = new LinkedHashMap<>();

        if (EXCLUDED_TABLES.contains(tableName.toLowerCase())) {
            response.put("success", false);
            response.put("error", "Table non autorisée : " + tableName);
            return ResponseEntity.badRequest().body(response);
        }

        try (Connection conn = dataSource.getConnection()) {

            List<Map<String, Object>> rulesSame = new ArrayList<>();
            String sqlSame = """
                SELECT id, table_name, NULL as table_2,
                       column_1, column_2, rule, value_to_compare,
                       rule_description, 'same' as rule_type
                FROM compare_rule_same_table
                WHERE LOWER(table_name) = LOWER(?)
                  AND rule IS NOT NULL
                ORDER BY column_1, id
            """;
            try (PreparedStatement ps = conn.prepareStatement(sqlSame)) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rulesSame.add(mapRuleRow(rs, "same"));
                    }
                }
            }

            List<Map<String, Object>> rulesDiff = new ArrayList<>();
            String sqlDiff = """
                SELECT id, table_1 as table_name, table_2,
                       column_1, column_2, rule, NULL as value_to_compare,
                       rule_description, 'diff' as rule_type
                FROM compare_rule_diff_table
                WHERE LOWER(table_1) = LOWER(?)
                  AND rule IS NOT NULL
                ORDER BY column_1, id
            """;
            try (PreparedStatement ps = conn.prepareStatement(sqlDiff)) {
                ps.setString(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        rulesDiff.add(mapRuleRow(rs, "diff"));
                    }
                }
            }

            Map<String, Map<String, Object>> groupedByColumn = new LinkedHashMap<>();
            List<Map<String, Object>> allRules = new ArrayList<>();
            allRules.addAll(rulesSame);
            allRules.addAll(rulesDiff);

            for (Map<String, Object> rule : allRules) {
                String columnName = (String) rule.get("column1");
                groupedByColumn.computeIfAbsent(columnName, c -> {
                    Map<String, Object> cd = new LinkedHashMap<>();
                    cd.put("columnName", c);
                    cd.put("rules", new ArrayList<Map<String, Object>>());
                    return cd;
                });
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> colRules =
                        (List<Map<String, Object>>) groupedByColumn.get(columnName).get("rules");
                colRules.add(rule);
            }

            response.put("success", true);
            response.put("tableName", tableName);
            response.put("columns", new ArrayList<>(groupedByColumn.values()));
            response.put("totalRules", allRules.size());
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Erreur récupération règles: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    // ════════════════════════════════════════════════════════════════
    // ENDPOINT 4 : Sauvegarder le plan de correction
    // ════════════════════════════════════════════════════════════════

    @PostMapping("/correction-plan")
    public ResponseEntity<Map<String, Object>> saveCorrectionPlan(
            @RequestBody Map<String, Object> requestBody) {

        Map<String, Object> response = new HashMap<>();
        try {
            Integer sourceIterationId = (Integer) requestBody.get("sourceIterationId");
            Integer targetIterationId = (Integer) requestBody.get("targetIterationId");
            String  selectedBy        = (String)  requestBody.get("selectedBy");

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> itemsToCorrect =
                    (List<Map<String, Object>>) requestBody.get("itemsToCorrect");

            if (sourceIterationId == null || targetIterationId == null) {
                response.put("success", false);
                response.put("error", "sourceIterationId et targetIterationId sont requis");
                return ResponseEntity.badRequest().body(response);
            }

            // Supprimer ancienne config pour cette itération
            iterationConfigRepository.deleteByIterationId(targetIterationId);

            LocalDateTime now = LocalDateTime.now();
            List<BscsIterationConfig> configsToSave = new ArrayList<>();

            for (Map<String, Object> item : itemsToCorrect) {
                String  tableName  = item.get("tableName") != null
                        ? ((String) item.get("tableName")).trim().toLowerCase() : null;
                String  columnName = (String)  item.get("columnName");
                String  rule       = (String)  item.get("rule");

                String ruleOrigin = (String) item.get("ruleOrigin");
                if (ruleOrigin == null || ruleOrigin.trim().isEmpty()) {
                    ruleOrigin = "UNKNOWN";
                } else {
                    ruleOrigin = ruleOrigin.toUpperCase();
                    if (!ruleOrigin.equals("SAME") && !ruleOrigin.equals("DIFF")) {
                        ruleOrigin = "UNKNOWN";
                    }
                }

                Integer ruleId = null;
                if (item.get("ruleId") != null) {
                    try {
                        ruleId = ((Number) item.get("ruleId")).intValue();
                    } catch (Exception e) {
                        log.warning("ruleId invalide pour " + tableName + "." + columnName);
                    }
                }

                Integer batchSize  = item.get("batchSize") != null
                        ? ((Number) item.get("batchSize")).intValue() : 0;

                boolean exists = iterationConfigRepository
                        .existsByIterationIdAndTableNameAndColumnNameAndRule(
                                targetIterationId, tableName, columnName, rule);

                if (!exists) {
                    BscsIterationConfig config = new BscsIterationConfig();
                    config.setIterationId(targetIterationId);
                    config.setTableName(tableName);
                    config.setColumnName(columnName);
                    config.setRule(rule);
                    config.setRuleOrigin(ruleOrigin);
                    config.setRuleId(ruleId);
                    config.setFlagToCheck(true);
                    config.setSelectedBy(selectedBy != null ? selectedBy : "agent");
                    config.setSelectedAt(now);
                    config.setBatchSize(batchSize);
                    config.setOffsetCurrent(0);
                    config.setRunId("PLAN_FROM_ITER_" + sourceIterationId);
                    configsToSave.add(config);

                    log.info(String.format(
                            "✅ Itération sauvegardée : [%s.%s] rule=%s | origin=%s | ruleId=%d",
                            tableName, columnName, rule, ruleOrigin, ruleId
                    ));
                }
            }

            if (!configsToSave.isEmpty()) {
                iterationConfigRepository.saveAll(configsToSave);
            }

            long sameCount = configsToSave.stream()
                    .filter(c -> "SAME".equals(c.getRuleOrigin())).count();
            long diffCount = configsToSave.stream()
                    .filter(c -> "DIFF".equals(c.getRuleOrigin())).count();

            response.put("success", true);
            response.put("correctionCount", configsToSave.size());
            response.put("targetIterationId", targetIterationId);
            response.put("sameRules", sameCount);
            response.put("diffRules", diffCount);
            response.put("message", String.format(
                    "%d élément(s) configurés (SAME=%d, DIFF=%d)",
                    configsToSave.size(), sameCount, diffCount));

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            response.put("success", false);
            response.put("error", "Erreur: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }
    @PostMapping("/trigger-airflow")
    public ResponseEntity<Map<String, Object>> triggerAirflow(
            @RequestBody Map<String, Object> requestBody) {

        Map<String, Object> response = new LinkedHashMap<>();

        try {
            Integer iterationId = (Integer) requestBody.get("iterationId");
            String  triggeredBy = (String)  requestBody.get("triggeredBy");

            if (iterationId == null) {
                response.put("success", false);
                response.put("error", "iterationId est requis");
                return ResponseEntity.badRequest().body(response);
            }

            List<BscsIterationConfig> config = iterationConfigRepository.findByIterationId(iterationId);
            if (config.isEmpty()) {
                response.put("success", false);
                response.put("error", "Aucune configuration trouvée pour l'itération " + iterationId);
                return ResponseEntity.badRequest().body(response);
            }

            log.info("=== TRIGGER AIRFLOW ===");
            log.info("URL Airflow : " + airflowApiUrl + " | DAG : " + airflowDagId + " | Iteration : " + iterationId);

            RestTemplate restTemplate = buildRestTemplate();

            String apiVersion = resolveAirflowApiVersion(restTemplate);
            String apiBase    = airflowApiUrl + apiVersion;
            log.info("Version API détectée : " + apiVersion);
            response.put("airflowApiVersion", apiVersion);

            HttpHeaders authHeaders = buildAuthHeaders(restTemplate, apiVersion);

            try {
                String unpauseUrl = apiBase + "/dags/" + airflowDagId;
                Map<String, Object> unpauseBody = Map.of("is_paused", false);
                HttpEntity<Map<String, Object>> unpauseEntity = new HttpEntity<>(unpauseBody, authHeaders);
                restTemplate.exchange(unpauseUrl, HttpMethod.PATCH, unpauseEntity, Map.class);
                log.info("DAG unpausé avec succès");
            } catch (Exception unpauseEx) {
                log.warning("Unpause ignoré : " + unpauseEx.getMessage());
            }

            Map<String, Object> dagRunConf = new LinkedHashMap<>();
            dagRunConf.put("iteration_id", iterationId);
            dagRunConf.put("triggered_by", triggeredBy != null ? triggeredBy : "interface_agent");

            Map<String, Object> airflowBody = new LinkedHashMap<>();
            airflowBody.put("conf", dagRunConf);
            airflowBody.put("dag_run_id", "manual_iter_" + iterationId + "_" + System.currentTimeMillis());
            airflowBody.put("logical_date",
                    java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC)
                            .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")));

            String triggerUrl = apiBase + "/dags/" + airflowDagId + "/dagRuns";
            log.info("Trigger URL : " + triggerUrl);

            HttpEntity<Map<String, Object>> triggerEntity = new HttpEntity<>(airflowBody, authHeaders);
            ResponseEntity<Map> airflowResponse = restTemplate.postForEntity(triggerUrl, triggerEntity, Map.class);

            if (airflowResponse.getStatusCode().is2xxSuccessful() && airflowResponse.getBody() != null) {
                Map<?, ?> body = airflowResponse.getBody();
                Object execDate = body.get("execution_date") != null
                        ? body.get("execution_date") : body.get("logical_date");
                response.put("success",       true);
                response.put("dagRunId",      body.get("dag_run_id"));
                response.put("state",         body.get("state"));
                response.put("executionDate", execDate);
                response.put("iterationId",   iterationId);
                response.put("message",       "DAG lancé — itération " + iterationId);
                log.info("DAG déclenché — dagRunId: " + body.get("dag_run_id"));
            } else {
                response.put("success", false);
                response.put("error", "Airflow statut inattendu: " + airflowResponse.getStatusCode());
            }

            return ResponseEntity.ok(response);

        } catch (org.springframework.web.client.HttpClientErrorException ex) {
            log.severe("Erreur HTTP Airflow : " + ex.getStatusCode() + " — " + ex.getResponseBodyAsString());
            response.put("success", false);
            response.put("error",   "Erreur Airflow " + ex.getStatusCode() + ": " + ex.getResponseBodyAsString());
            return ResponseEntity.status(500).body(response);
        } catch (org.springframework.web.client.ResourceAccessException ex) {
            log.severe("Airflow inaccessible : " + ex.getMessage());
            response.put("success", false);
            response.put("error",   "Impossible de joindre Airflow sur " + airflowApiUrl);
            return ResponseEntity.status(500).body(response);
        } catch (Exception e) {
            log.severe("Erreur inattendue : " + e.getClass().getName() + " — " + e.getMessage());
            response.put("success", false);
            response.put("error",   "Erreur: " + e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    // ─── Auth helpers ──────────────────────────────────────────

    private HttpHeaders buildAuthHeaders(RestTemplate restTemplate, String apiVersion) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        if ("/api/v2".equals(apiVersion)) {
            try {
                String tokenUrl = airflowApiUrl + "/auth/token";
                Map<String, String> credentials = new LinkedHashMap<>();
                credentials.put("username", airflowUser);
                credentials.put("password", airflowPassword);

                HttpEntity<Map<String, String>> tokenRequest = new HttpEntity<>(credentials, headers);
                ResponseEntity<Map> tokenResponse = restTemplate.postForEntity(tokenUrl, tokenRequest, Map.class);

                if (tokenResponse.getStatusCode().is2xxSuccessful() && tokenResponse.getBody() != null) {
                    String token = (String) tokenResponse.getBody().get("access_token");
                    headers.set("Authorization", "Bearer " + token);
                } else {
                    throw new RuntimeException("Token endpoint a retourné " + tokenResponse.getStatusCode());
                }
            } catch (Exception e) {
                throw new RuntimeException("Authentification Airflow 3.x échouée : " + e.getMessage(), e);
            }
        } else {
            String encoded = Base64.getEncoder()
                    .encodeToString((airflowUser + ":" + airflowPassword).getBytes());
            headers.set("Authorization", "Basic " + encoded);
        }

        return headers;
    }

    private String resolveAirflowApiVersion(RestTemplate restTemplate) {
        try {
            HttpHeaders h = new HttpHeaders();
            h.setContentType(MediaType.APPLICATION_JSON);
            ResponseEntity<Map> resp = restTemplate.exchange(
                    airflowApiUrl + "/api/v2/version",
                    HttpMethod.GET, new HttpEntity<>(h), Map.class);
            if (resp.getStatusCode().is2xxSuccessful()) {
                return "/api/v2";
            }
        } catch (Exception e) {
            log.info("Airflow v2 non disponible → fallback /api/v1");
        }
        return "/api/v1";
    }

    // ─── Helper : mapping ResultSet → Map règle ───────────────

    private Map<String, Object> mapRuleRow(ResultSet rs, String ruleType) throws SQLException {
        String column1         = rs.getString("column_1");
        String column2         = rs.getString("column_2");
        String ruleOp          = rs.getString("rule");
        String valueToCompare  = rs.getString("value_to_compare");
        String table2          = rs.getString("table_2");

        String ruleLabel;
        if (ruleOp != null && List.of("IN", "LIKE", "REGEX").contains(ruleOp)) {
            String actualVal = valueToCompare != null ? valueToCompare : column2;
            ruleLabel = (column1 + " " + ruleOp + " " + (actualVal != null ? actualVal : "")).trim();
        } else if (ruleOp != null && List.of("BOTH","CORRELATE","LENGTH","SEQUENCE","OR","MAXDET","CORR").contains(ruleOp)) {
            ruleLabel = (column1 + " " + ruleOp + " " + (column2 != null ? column2 : "")).trim();
        } else {
            String rightSide = valueToCompare != null ? valueToCompare : (column2 != null ? column2 : "");
            ruleLabel = (column1 + " " + ruleOp + " " + rightSide).trim();
        }

        Map<String, Object> rule = new LinkedHashMap<>();
        rule.put("id",              rs.getInt("id"));
        rule.put("tableName",       rs.getString("table_name"));
        rule.put("table2",          table2);
        rule.put("column1",         column1);
        rule.put("column2",         column2);
        rule.put("ruleOp",          ruleOp);
        rule.put("valueToCompare",  valueToCompare);
        rule.put("ruleLabel",       ruleLabel);
        rule.put("ruleDescription", rs.getString("rule_description"));
        rule.put("ruleType",        ruleType);
        return rule;
    }
}