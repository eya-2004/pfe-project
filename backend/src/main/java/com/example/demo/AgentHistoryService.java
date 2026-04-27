package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
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

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public List<AgentIterationSummary> getAllAgentsHistorySummary() {
        List<User> agents = userRepository.findByRole(Role.AGENT_MIGRATION);
        List<AgentIterationSummary> summaryList = new ArrayList<>();

        for (User agent : agents) {
            String selectedBy = agent.getMail();

            Long totalIterations = iterationRepository.countBySelectedBy(selectedBy);
            LocalDateTime lastDateTime = iterationRepository.findLastIterationDateBySelectedBy(selectedBy);
            String lastDate = lastDateTime != null ? lastDateTime.format(FORMATTER) : null;

            summaryList.add(new AgentIterationSummary(
                    agent.getId(),
                    agent.getFirstname(),
                    agent.getLastname(),
                    agent.getMail(),
                    totalIterations != null ? totalIterations.intValue() : 0,
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
}