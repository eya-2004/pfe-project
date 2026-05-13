package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * Repository pour l'entité BSCS_ITERATION_CONFIG
 * Gère les configurations d'itération (règles sélectionnées pour correction)
 */
@Repository
public interface IterationConfigRepository extends JpaRepository<BscsIterationConfig, Integer> {

    List<BscsIterationConfig> findByIterationId(Integer iterationId);

    List<BscsIterationConfig> findByIterationIdAndFlagToCheck(Integer iterationId, Boolean flagToCheck);

    boolean existsByIterationIdAndTableNameAndColumnNameAndRule(
            Integer iterationId,
            String tableName,
            String columnName,
            String rule
    );

    @Modifying
    @Transactional
    @Query("DELETE FROM BscsIterationConfig ic WHERE ic.iterationId = :iterationId")
    void deleteByIterationId(@Param("iterationId") Integer iterationId);

    /** Tous les IDs d'itérations (ordre décroissant) */
    @Query("SELECT DISTINCT ic.iterationId FROM BscsIterationConfig ic ORDER BY ic.iterationId DESC")
    List<Integer> findAllIterationIds();

    @Query("SELECT CASE WHEN COUNT(c) > 0 THEN true ELSE false END FROM BscsIterationConfig c WHERE c.iterationId = :iterationId AND c.hasCorrection = true")
    Optional<Boolean> findHasCorrectionByIterationId(@Param("iterationId") Integer iterationId);
    @Query("SELECT MAX(ic.iterationId) FROM BscsIterationConfig ic")
    Optional<Integer> findMaxIterationId();
}


