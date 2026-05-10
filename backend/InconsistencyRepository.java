package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface InconsistencyRepository extends JpaRepository<Inconsistency, Integer> {

    // === REQUÊTES PAR ITÉRATION ===
    List<Inconsistency> findByIterationId(Integer iterationId);

    List<Inconsistency> findByIterationIdAndTableName(Integer iterationId, String tableName);
    List<Inconsistency> findByIterationIdAndRunId(Integer iterationId, String runId);
    List<Inconsistency> findByIterationIdAndTableNameAndColumnName(
            Integer iterationId, String tableName, String columnName);
    List<Inconsistency> findByRunId(String runId);
    @Query(value = "SELECT run_id, MAX(execution_date) as execution_date " +
            "FROM bscs_detected_inconsistency " +  // ← remplace par ton vrai nom de table
            "WHERE iteration_id = :iterationId " +
            "GROUP BY run_id " +
            "ORDER BY MAX(execution_date) DESC",
            nativeQuery = true)
    List<Object[]> findRunsByIterationId(@Param("iterationId") Integer iterationId);
    @Query("SELECT DISTINCT i.iterationId FROM Inconsistency i ORDER BY i.iterationId DESC")
    List<Integer> findAllIterationIds();

    @Query("SELECT MAX(i.iterationId) FROM Inconsistency i")
    Optional<Integer> findMaxIterationId();

    // === REQUÊTES POUR LE FILTRAGE ===
    @Query("SELECT DISTINCT i.tableName FROM Inconsistency i WHERE i.iterationId = :iterationId")
    List<String> findDistinctTableNamesByIterationId(@Param("iterationId") Integer iterationId);

    @Query("SELECT DISTINCT i.errorCategory FROM Inconsistency i WHERE i.iterationId = :iterationId")
    List<String> findDistinctErrorCategoriesByIterationId(@Param("iterationId") Integer iterationId);

    @Query("SELECT DISTINCT i.columnName FROM Inconsistency i " +
            "WHERE i.iterationId = :iterationId AND i.tableName = :tableName")
    List<String> findDistinctColumnsByTableAndIteration(
            @Param("iterationId") Integer iterationId,
            @Param("tableName") String tableName);

    @Query("UPDATE BscsIterationConfig ic SET ic.offsetCurrent = :offset WHERE ic.iterationId = :iterationId AND ic.tableName = :tableName AND ic.flagToCheck = TRUE")
    void updateOffset(@Param("iterationId") Integer iterationId,
                      @Param("tableName") String tableName,
                      @Param("offset") Integer offset);
}