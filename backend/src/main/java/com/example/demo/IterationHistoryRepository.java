package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface IterationHistoryRepository extends JpaRepository<BscsIterationConfig, Integer> {

    @Query("SELECT DISTINCT new com.example.demo.IterationSummaryDto(i.iterationId, i.runId, i.selectedAt) " +
            "FROM BscsIterationConfig i " +
            "WHERE i.selectedBy = :selectedBy " +
            "ORDER BY i.iterationId ASC, i.selectedAt ASC")
    List<IterationSummaryDto> findDistinctIterationsBySelectedBy(@Param("selectedBy") String selectedBy);

    @Query("SELECT COUNT(DISTINCT i.iterationId) FROM BscsIterationConfig i WHERE i.selectedBy = :selectedBy")
    Long countBySelectedBy(@Param("selectedBy") String selectedBy);

    @Query("SELECT MAX(i.selectedAt) FROM BscsIterationConfig i WHERE i.selectedBy = :selectedBy")
    LocalDateTime findLastIterationDateBySelectedBy(@Param("selectedBy") String selectedBy);
}