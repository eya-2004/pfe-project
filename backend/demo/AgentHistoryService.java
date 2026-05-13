package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
public class AgentHistoryService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private IterationHistoryRepository iterationRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public List<AgentIterationSummary> getAllAgentsHistorySummary() {
        List<User> agents = userRepository.findByRole(Role.AGENT_MIGRATION);
        List<AgentIterationSummary> summaryList = new ArrayList<>();

        for (User agent : agents) {
            String mail = agent.getMail();

            // Compteur détection (table existante)
            Long detectionCount = iterationRepository.countBySelectedBy(mail);

            // Compteur correction : itérations distinctes ayant un detection_run_id non null/vide
            String corrSql = """
                SELECT COUNT(DISTINCT iteration_id)
                FROM correction_iteration_config
                WHERE created_by = ?
                  AND detection_run_id IS NOT NULL
                  AND detection_run_id <> ''
            """;
            Long correctionCount = jdbcTemplate.queryForObject(corrSql, Long.class, mail);

            LocalDateTime lastDetection = iterationRepository.findLastIterationDateBySelectedBy(mail);
            String lastDate = lastDetection != null ? lastDetection.format(FORMATTER) : null;

            summaryList.add(new AgentIterationSummary(
                    agent.getId(),
                    agent.getFirstname(),
                    agent.getLastname(),
                    mail,
                    detectionCount != null ? detectionCount.intValue() : 0,
                    correctionCount != null ? correctionCount.intValue() : 0,
                    lastDate
            ));
        }
        return summaryList;
    }

    public List<IterationSummaryDto> getAgentIterations(Long agentId) {
        User agent = userRepository.findById(agentId)
                .orElseThrow(() -> new RuntimeException("Agent non trouvé : " + agentId));
        return iterationRepository.findDistinctIterationsBySelectedBy(agent.getMail());
    }


    public List<CorrectionIterationDto> getAgentCorrectionIterations(Long agentId) {
        User agent = userRepository.findById(agentId)
                .orElseThrow(() -> new RuntimeException("Agent non trouvé : " + agentId));

        String mail = agent.getMail();

        String sql = """
            SELECT
                id,
                rule_id,
                to_execute,
                created_by,
                dag_run_id,
                DATE_FORMAT(created_at, '%d/%m/%Y %H:%i') AS created_at,
                iteration_id,
                detection_run_id,
                detection_rule_label,
                target_column
            FROM correction_iteration_config
            WHERE created_by = ?
              AND detection_run_id IS NOT NULL
              AND detection_run_id <> ''
            ORDER BY iteration_id DESC, id DESC
        """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> new CorrectionIterationDto(
                rs.getLong("id"),
                rs.getLong("rule_id"),
                rs.getString("to_execute"),
                rs.getString("created_by"),
                rs.getString("dag_run_id"),
                rs.getString("created_at"),
                rs.getInt("iteration_id"),
                rs.getString("detection_run_id"),
                rs.getString("detection_rule_label"),
                rs.getString("target_column")
        ), mail);
    }
}