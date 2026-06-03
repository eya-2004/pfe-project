package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;


@Repository
public interface IterationConfigRepository extends JpaRepository<BscsIterationConfig, Integer> {

    List<BscsIterationConfig> findByIterationId(Integer iterationId);


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
    @Query(value = """
    SELECT b.table_name, b.offset_current
    FROM bscs_iteration_config b
    INNER JOIN (
        SELECT table_name, MAX(iteration_id) AS last_iter
        FROM bscs_iteration_config
        GROUP BY table_name
    ) latest ON b.table_name = latest.table_name
            AND b.iteration_id = latest.last_iter
    GROUP BY b.table_name, b.offset_current
    """, nativeQuery = true)
List<Object[]> findOffsetByLatestIteration();
    


    @Query("SELECT MAX(ic.iterationId) FROM BscsIterationConfig ic")
    Optional<Integer> findMaxIterationId();
    }


