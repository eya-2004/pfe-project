package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface BssMigrationRuleRepository extends JpaRepository<BssMigrationRule, Long> {
    List<BssMigrationRule> findBySourceTableIgnoreCaseAndSourceColumnIgnoreCase(
    String sourceTable, String sourceColumn
);
}