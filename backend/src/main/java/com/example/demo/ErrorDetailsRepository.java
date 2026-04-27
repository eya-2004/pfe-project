package com.example.demo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface ErrorDetailsRepository extends JpaRepository<BscsErrorDetail, Long> {

    List<BscsErrorDetail> findByCorrectionStatus(String correctionStatus);

    List<BscsErrorDetail> findByTableName(String tableName);

    List<BscsErrorDetail> findByTableNameAndCorrectionStatus(String tableName, String correctionStatus);

    Optional<BscsErrorDetail> findByTableNameAndRowPkValue(String tableName, String rowPkValue);

    List<BscsErrorDetail> findBySourceRunId(String sourceRunId);

    @Query("SELECT DISTINCT ed.tableName FROM BscsErrorDetail ed")
    List<String> findDistinctTableNames();

    @Query("SELECT ed.correctionStatus, COUNT(ed) as count FROM BscsErrorDetail ed GROUP BY ed.correctionStatus")
    List<Map<String, Object>> getCorrectionStatusStats();

    @Query("SELECT ed.tableName, ed.correctionStatus, COUNT(ed) as count FROM BscsErrorDetail ed " +
           "GROUP BY ed.tableName, ed.correctionStatus")
    List<Map<String, Object>> getCorrectionStatusByTable();

    @Query("SELECT DATE(ed.exportDate) as exportDate, COUNT(ed) as count FROM BscsErrorDetail ed " +
           "GROUP BY DATE(ed.exportDate) ORDER BY exportDate DESC")
    List<Map<String, Object>> getDailyExportStats();

    @Query("SELECT ed.tableName, COUNT(ed) as count FROM BscsErrorDetail ed " +
           "WHERE ed.correctionStatus = 'PENDING' GROUP BY ed.tableName ORDER BY count DESC")
    List<Map<String, Object>> getPendingErrorsByTable();

    @Query("SELECT ed.correctedBy, COUNT(ed) as count FROM BscsErrorDetail ed " +
           "WHERE ed.correctedBy IS NOT NULL GROUP BY ed.correctedBy ORDER BY count DESC")
    List<Map<String, Object>> getCorrectionsByUser();

    @Query("SELECT ed.sourceRunId, COUNT(ed) as count FROM BscsErrorDetail ed " +
           "GROUP BY ed.sourceRunId ORDER BY count DESC")
    List<Map<String, Object>> getErrorsByRun();
}