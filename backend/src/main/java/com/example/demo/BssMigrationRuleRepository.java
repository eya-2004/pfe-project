package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface BssMigrationRuleRepository extends JpaRepository<BssMigrationRule, Long> {

    @Query("SELECT DISTINCT r.sourceTable FROM BssMigrationRule r ORDER BY r.sourceTable")
    List<String> findDistinctSourceTables();

    List<BssMigrationRule> findBySourceTableOrderBySourceColumn(String sourceTable);

    @Query("SELECT COUNT(r) FROM BssMigrationRule r WHERE r.sourceTable = ?1")
    long countBySourceTable(String sourceTable);
}