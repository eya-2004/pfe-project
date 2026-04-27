package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface QualityRunRepository extends JpaRepository<QualityRun, Integer> {

    Optional<QualityRun> findByRunId(String runId);

    List<QualityRun> findByIterationId(Integer iterationId);

    List<QualityRun> findByStatus(String status);

    @Query("SELECT q FROM QualityRun q WHERE q.status = 'SUCCESS' ORDER BY q.executionDate DESC")
    List<QualityRun> findAllSuccessfulRunsOrderByDateDesc();

    @Query("SELECT q FROM QualityRun q WHERE DATE(q.executionDate) = DATE(:date)")
    List<QualityRun> findByExecutionDate(@Param("date") LocalDateTime date);

    @Query("SELECT q FROM QualityRun q WHERE q.iterationId = :iterationId ORDER BY q.executionDate DESC")
    List<QualityRun> findByIterationIdOrderByDateDesc(@Param("iterationId") Integer iterationId);

    @Query("SELECT q FROM QualityRun q WHERE q.iterationId = :iterationId AND q.status = 'SUCCESS'")
    List<QualityRun> findSuccessfulByIterationId(@Param("iterationId") Integer iterationId);

    @Query("SELECT DISTINCT q.iterationId FROM QualityRun q ORDER BY q.iterationId DESC")
    List<Integer> findAllIterationIds();
    @Query("SELECT MAX(q.iterationId) FROM QualityRun q")
    Optional<Integer> findMaxIterationId();
    List<QualityRun> findByIterationIdOrderByExecutionDateDesc(Integer iterationId);
}