package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.*;

@Service
public class PostCorrectionService {

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

    // =========================================================================
    // Lance le DAG ETL avec detection_run_id dans le conf
    // =========================================================================
    public Map<String, Object> launchEtlDagWithDetectionRunId(
            Integer iterationId, String detectionRunId, String triggeredBy) {

        // 1. Vérification de l'itération
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

            // 4. Mise à jour en base
            try {
                jdbcTemplate.update(
                        "UPDATE correction_iteration_config SET detection_run_id = ?, dag_run_id = ? WHERE iteration_id = ?",
                        detectionRunId, dagRunId, iterationId
                );
            } catch (Exception e) {
                System.err.println("❌ Erreur mise à jour base : " + e.getMessage());
                // DAG lancé mais base non mise à jour — on log sans bloquer
            }

            // 5. Retour
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
    // =========================================================================
    // Résultats avant/après par règle pour un run_id de détection
    // =========================================================================
    public List<Map<String, Object>> getBeforeAfterResults(String detectionRunId) {
        String sql = """
            SELECT
                id,
                table_name,
                column_name,
                rule,
                rule_description,
                error_category,
                count_source,
                nb_violations,
                avant_correction,
                apres_correction,
                CASE
                    WHEN avant_correction IS NOT NULL AND apres_correction IS NOT NULL
                        THEN avant_correction - apres_correction
                    ELSE NULL
                END AS lignes_corrigees,
                CASE
                    WHEN avant_correction IS NOT NULL AND avant_correction > 0
                         AND apres_correction IS NOT NULL
                        THEN ROUND((avant_correction - apres_correction) * 100.0 / avant_correction, 2)
                    ELSE NULL
                END AS taux_amelioration
            FROM bscs_detected_inconsistency
            WHERE run_id = ?
            ORDER BY table_name, rule
        """;
        return jdbcTemplate.queryForList(sql, detectionRunId);
    }

    // =========================================================================
    // Résumé agrégé par table
    // =========================================================================
    public List<Map<String, Object>> getTableSummary(String detectionRunId) {
        String sql = """
            SELECT
                table_name,
                COUNT(*)                            AS nb_regles,
                SUM(avant_correction)               AS total_violations_avant,
                SUM(apres_correction)               AS total_violations_apres,
                SUM(avant_correction - apres_correction) AS total_corrigees,
                CASE
                    WHEN SUM(avant_correction) > 0
                        THEN ROUND(
                            SUM(avant_correction - apres_correction) * 100.0
                            / SUM(avant_correction), 2)
                    ELSE 0
                END AS taux_amelioration_pct,
                SUM(CASE WHEN apres_correction = 0 THEN 1 ELSE 0 END) AS regles_100pct_corrigees,
                SUM(CASE WHEN apres_correction = avant_correction THEN 1 ELSE 0 END)
                    AS regles_non_ameliorees
            FROM bscs_detected_inconsistency
            WHERE run_id            = ?
              AND avant_correction  IS NOT NULL
              AND apres_correction  IS NOT NULL
            GROUP BY table_name
            ORDER BY taux_amelioration_pct DESC
        """;
        return jdbcTemplate.queryForList(sql, detectionRunId);
    }

    // =========================================================================
    // Runs de détection disponibles (qui ont avant_correction rempli)
    // =========================================================================
    public List<Map<String, Object>> getAvailableDetectionRuns() {
        String sql = """
            SELECT
                bdi.run_id,
                bdi.iteration_id,
                bqr.execution_date,
                bqr.triggered_by,
                COUNT(DISTINCT bdi.table_name)      AS nb_tables,
                SUM(bdi.nb_violations)              AS total_violations,
                MAX(bdi.avant_correction) IS NOT NULL AS has_avant_correction,
                MAX(bdi.apres_correction) IS NOT NULL AS has_apres_correction
            FROM bscs_detected_inconsistency bdi
            LEFT JOIN bscs_quality_run bqr ON bqr.run_id = bdi.run_id
            GROUP BY bdi.run_id, bdi.iteration_id, bqr.execution_date, bqr.triggered_by
            ORDER BY bqr.execution_date DESC
        """;
        return jdbcTemplate.queryForList(sql);
    }
}