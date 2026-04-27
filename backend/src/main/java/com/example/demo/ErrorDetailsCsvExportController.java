package com.example.demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;

@RestController
@RequestMapping("/api/export")
public class ErrorDetailsCsvExportController {

    @Autowired
    private ErrorDetailsRepository repository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private final Map<String, List<String>> tableColumns = new HashMap<>();

    public ErrorDetailsCsvExportController() {
        // Fusion des deux listes pour BSCS_BILLING_ACCOUNT pour éviter la clé en double
        List<String> billingAccountColumns = new ArrayList<>(Arrays.asList(
        		"BA_ENTRY_DATE",
        		"BA_VERS_STATUT",
        		"BA_VERS_VALID_FROM",
        		"BILL_MEDIUM",
        		"BILL_MEDIUM_DESC",
        		"BILLING_ACCOUNT_CODE",
        		"BILLING_ACCOUNT_ID",
        		"BILLING_ACCOUNT_NAME",
        		"BILLING_ACCOUNT_PRIMAIRE",
        		"CALL_DETAIL_FLAG",
        		"CURRENCY_DESC",
        		"CURRENCY_ID",
        		"CUSTOMER_ID",
        		"INVOICING_IND",
        		"LAST_BILLED_DATE"
        		
        ));
        
                
        
        
        tableColumns.put("BSCS_BILLING_ACCOUNT", billingAccountColumns);
        
        tableColumns.put("BSCS_BILLING_ACCOUNT_ASSIGN", Arrays.asList(
            "BILLING_ACCOUNT_ID", "CONTRACT_ID", "CUSTOMER_ID", "INVOICING_IND",
            "VALID_FROM"
        ));

        tableColumns.put("BSCS_CUSTOMER", Arrays.asList(
            "BANKACCOUNTHOLDER", "BANKACCOUNTHOLDERADDRESS", "BANKACCOUNTNUMBER", "BIC",
            "BILLING_ADRESSE", "BILLINGCYCLE", "BRANDSYMBOL", "CIVILITE", "COSTCENTER",
            "COUNTRY", "COUNTRYCODEOFBIRTH", "CREDITPROFILEVALIDFROM", "CREDITPROFILEVALIDTO",
            "CREDITRISKRATINGINTVALUE", "CREDITSCOREINTVALUE", "CREDITSCOREVALIDFROM",
            "CSMODDATE", "CURRENCY", "CUSTCODE", "CUSTOMER_ID", "CUSTOMER_ID_HIGH",
            "DATEOFBIRTH", "DESCRIPTION", "EMAILSPEC", "FIRSTNAME", "ID_PARTY",
            "ID_SOURCE", "IDDOCUMENTNUMBER", "JOBTITLE", "LANGUAGE", "LANGUAGEPREF",
            "LASTNAME", "LEGALNAME", "MARKETSEGMENTS", "ORGANIZATIONTYPE",
            "PARTYSPECSYMBOL", "PAYMENTCARDEXPIRATIONDATE", "PAYMENTCARDHOLDER",
            "PAYMENTCARDNUMBER", "PAYMENTCARDTYPE", "PAYMENTIDENTIFIER", "PAYMENTTERMS",
            "PAYMENTTYPE", "PAYMNTRESP", "PERMANENTADDRESS_ID", "PHONESPEC",
            "PLACEOFBIRTH", "PLACEOFREGISTRATION", "PREFERREDMEDIUM", "ROLESTATESYMBOL",
            "ROLETYPE", "TITLE", "TYPEOFIDDOCUMENT", "USAGE_ADRESSE", "VALIDFROM",
            "VALIDTO", "VENDORSYMBOL"
        ));

        tableColumns.put("BSCS_CUSTOMER_TAX_EXEMPT", Arrays.asList(
            "CUSTOMER_ID", "EXEMPT_RATE", "EXEMPT_STATUS", "EXPIRATION_DATE",
            "TAXCODE", "TAXCODE_NAME", "VALID_FROM"
        ));
        
        tableColumns.put("BSCS_FINDOCS", Arrays.asList(
            "BILLING_ACCOUNT_ID", "COMPLAINT", "CURRENCY", "CURRENCYINITAMOUNT",
            "CSMODDATE", "CSMODDATETIME", "CSMODPROG", "CSMODUSER", "CUSTOMER_ID",
            "DOCTYPE", "DOCTYPE_DET", "DUEDATE", "EXTREFERENCE", "ID_FINDOC",
            "INITAMOUNT", "ISSUEDATE", "NETINITAMOUNT", "REFERENCE", "UNCLEAREDAMOUNT"
        ));

        tableColumns.put("BSCS_PAYMENTS", Arrays.asList(
            "AGENCE", "CANAL", "CSMODDATE", "CSMODDATETIME", "CSMODPROG", "CSMODUSER",
            "CUSTOMER_ID", "PAYMENT_AMOUNT", "PAYMENT_CURRENCY", "PAYMENT_DATE",
            "PAYMENT_ID", "PAYMENT_MODE", "PAYMENT_REFERENCE", "REV_PAYMENT_ID",
            "STATUS", "TRANSACTION_TYPE", "USERNAME"
        ));

        tableColumns.put("BSCS_PAYMENTS_DET", Arrays.asList(
            "AMOUNT_PAID", "CUSTOMER_ID", "ID", "ID_FINDOC", "PAYMENT_ID"
        ));

        tableColumns.put("BSCS_PLACE", Arrays.asList(
            "COUNTRY", "CSMODDATE", "CSMODDATETIME", "CSMODPROG", "CSMODUSER",
            "CUSTOMER_ID", "DESCRIPTION", "LOCALITY", "PLACE_ID", "PLACE_TYPE",
            "POSTALCODE", "STREET", "UPDATEFROM", "VALIDFROM"
        ));
        
        tableColumns.put("BSCS_PORTABILITY_IN", Arrays.asList(
            "ACTION_ID", "CONTRACT_ID", "CSMODDATE", "CSMODDATETIME", "CSMODPROG",
            "CSMODUSER", "CUSTOMER_ID", "DN_NUM", "ENTRY_DATE", "PORTING_DATE",
            "RAISON_PORTABILITY", "SOURCE_PLCODE", "STATUS", "TARGET_PLCODE"
        ));
        
        tableColumns.put("bscs_charge", Arrays.asList(
            "AMOUNT","AMOUNT_GROSS","CO_ID","CUSTOMER_ID","DESCRIPTION","ENTDATE",
            "GLCODE","PERIOD","SNCODE","SPCODE","TMCODE","VALID_FROM","VSCODE"
        ));
        
        tableColumns.put("BSCS_SERVICES", Arrays.asList(
            "BILLING_ACCOUNT_ID","COMMITMENTFROM","COMMITMENTTO","CONTRACT_ID",
            "CUSTOMER_ID","CUSTOMPRICEVALUE","CUSTOMPRICEVALUE_OT","ENTRY_DATE",
            "ID_SERVICE","ID_SERVICE_PARAMETERS","ID_SPCODE","PLACE_ID","RATEPLAN",
            "SERVICE_SHDES","STATUS","TYPE_PRODUCT","VALID_FROM_DATE",
            "VALIDFROM_PRICE","VALIDTO_PRICE"
        ));
        

        tableColumns.put("BSCS_SERVICES_PARAMETER", Arrays.asList(
            "CONTRACT_ID",
            "ID_SERVICE_PARAMETERS",
            "PARAMETER_ID",
            "PRM_DES",
            "PRM_NO",
            "PRM_SHDES",
            "PRM_VALID_FROM",
            "PRM_VALUE",
            "SCCODE",
            "SERVICE_SHDES"
        ));

        tableColumns.put("IXC_PAYMENT_PLAN", Arrays.asList(
            "CUSTOMER_ID",
            "ID_INSTALLEMENT",
            "ID_PAYMENT_PLAN",
            "ID_PROCESSUS_DUN",
            "INSTALLEMENT_AMOUNT",
            "INSTALLEMENT_STATUS",
            "INSTALLMENET_CREATE_DATE",
            "INSTALLMENET_DUE_DATE",
            "INVOICE_REFERENCE"
        ));

        tableColumns.put("BSCS_MEMOS", Arrays.asList(
            "MEMO_ID",
            "CONTRACT_ID",
            "CUSTOMER_ID",
            "MEMO_TYPE",
            "MEMO_TEXT",
            "CREATED_DATE",
            "CREATED_BY",
            "IS_INTERNAL",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("BSCS_RESOURCE_DIRECTORY", Arrays.asList(
            "DIRECTORY_ID",
            "DIRECTORY_NAME",
            "RESOURCE_TYPE",
            "DESCRIPTION",
            "STATUS",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("IXC_DUNPROCESS", Arrays.asList(
            "DUN_PROCESS_ID",
            "FINDOC_ID",
            "CUSTOMER_ID",
            "DUN_LEVEL",
            "DUN_DATE",
            "METHOD",
            "STATUS",
            "ATTEMPT_COUNT",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("CONTRACT_HISTORY", Arrays.asList(
            "HIST_ID",
            "CONTRACT_ID",
            "CHANGE_DATE",
            "CHANGE_TYPE",
            "FIELD_CHANGED",
            "OLD_VALUE",
            "NEW_VALUE",
            "REASON",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("CARRY_OVER", Arrays.asList(
            "CARRY_OVER_ID",
            "SUBSCRIPTION_ID",
            "BILLING_CYCLE_ID",
            "RESOURCE_TYPE",
            "AMOUNT_CARRIED_OVER",
            "EXPIRY_DATE",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("BSCS_SOUSCRIPTION", Arrays.asList(
            "SUBSCRIPTION_ID",
            "CUSTOMER_ID",
            "SERVICE_ID",
            "CONTRACT_ID",
            "SUBSCRIPTION_DATE",
            "START_DATE",
            "END_DATE",
            "STATUS",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("BSCS_RESOURCE_SIM", Arrays.asList(
            "SIM_ID",
            "SIM_ICCID",
            "SIM_IMSI",
            "SIM_PIN",
            "PUK",
            "STATUS",
            "ASSIGNMENT_DATE",
            "CONTRACT_ID",
            "SUPPLIER",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("BSCS_RESOURCE_PORT", Arrays.asList(
            "PORT_ID",
            "PORT_NUMBER",
            "STATUS",
            "EQUIPMENT_ID",
            "ASSIGNMENT_DATE",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("BSCS_PRE_ACTIVATION", Arrays.asList(
            "PRE_ACTIVATION_ID",
            "SUBSCRIPTION_ID",
            "CUSTOMER_ID",
            "PRE_ACTIVATION_DATE",
            "EXPECTED_ACTIVATION_DATE",
            "STATUS",
            "NOTES",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));

        tableColumns.put("BSCS_PORTABILITY_HIST", Arrays.asList(
            "HIST_ID",
            "ACTION_ID",
            "DN_NUM",
            "STATUS_CHANGE_DATE",
            "OLD_STATUS",
            "NEW_STATUS",
            "REASON",
            "CSMODDATE",
            "CSMODDATETIME",
            "CSMODPROG",
            "CSMODUSER"
        ));
    }

    @GetMapping("/csv")
    public ResponseEntity<byte[]> exportCsv(
            @RequestParam(required = false, defaultValue = "PENDING") String status,
            @RequestParam(required = false) String table
    ) {
        try {
            if (status == null || status.isBlank()) status = "PENDING";

            String tableNormalized = (table != null && !table.isBlank())
                    ? table.trim().toUpperCase() : null;

            System.out.println("[exportCsv] table='" + table + "' → normalisée='" + tableNormalized + "'");
            System.out.println("[exportCsv] status='" + status + "'");

            List<BscsErrorDetail> rows;
            if (tableNormalized != null) {
                rows = repository.findByTableNameAndCorrectionStatus(tableNormalized, status);
            } else {
                rows = repository.findByCorrectionStatus(status);
            }

            rows = rows.stream()
                .filter(r -> r.getColumnErrors() != null 
                         && !r.getColumnErrors().isBlank()
                         && !r.getColumnErrors().equals("{}")
                         && !r.getColumnErrors().equals("null"))
                .collect(java.util.stream.Collectors.toList());
            System.out.println("[exportCsv] Nombre de lignes trouvées: " + rows.size());
            rows.forEach(r -> System.out.println("  → table=" + r.getTableName()
                    + " pk=" + r.getRowPkValue()
                    + " status=" + r.getCorrectionStatus()));

            if (rows.isEmpty()) return ResponseEntity.noContent().build();

            if (tableNormalized != null) {
                // Export d'une seule table → un seul CSV avec toutes ses colonnes
                byte[] csv = buildCsvForRows(rows, tableNormalized);
                if (csv == null) return ResponseEntity.noContent().build();

                String date     = java.time.LocalDate.now().toString();
                String filename = table.trim().toLowerCase() + "_errors_" + date + ".csv";

                HttpHeaders responseHeaders = new HttpHeaders();
                responseHeaders.setContentType(MediaType.parseMediaType("text/csv; charset=UTF-8"));
                responseHeaders.setContentDisposition(
                        ContentDisposition.attachment().filename(filename).build());
                responseHeaders.setContentLength(csv.length);

                return ResponseEntity.ok().headers(responseHeaders).body(csv);

            } else {
                // Export de toutes les tables → on regroupe par table_name
                // et on génère un CSV unique avec table_name comme première colonne
                byte[] csv = buildCsvForAllTables(rows);
                if (csv == null) return ResponseEntity.noContent().build();

                String date     = java.time.LocalDate.now().toString();
                String filename = "all_tables_errors_" + date + ".csv";

                HttpHeaders responseHeaders = new HttpHeaders();
                responseHeaders.setContentType(MediaType.parseMediaType("text/csv; charset=UTF-8"));
                responseHeaders.setContentDisposition(
                        ContentDisposition.attachment().filename(filename).build());
                responseHeaders.setContentLength(csv.length);

                return ResponseEntity.ok().headers(responseHeaders).body(csv);
            }

        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * Construit un CSV pour un ensemble de lignes appartenant à UNE SEULE table.
     * Les colonnes sont toutes les colonnes de la table source (full_row_data).
     * Seules les lignes ayant des erreurs sont incluses.
     */
    private byte[] buildCsvForRows(List<BscsErrorDetail> rows, String tableName) throws Exception {
        if (rows.isEmpty()) return null;

        // ── Déterminer les colonnes de la table ──────────────────────────────
        List<String> dataColumns = getColumnsForTable(tableName, rows);

        // ── Headers ──────────────────────────────────────────────────────────
        List<String> headers = new ArrayList<>();
        headers.add("table_name");
        headers.add("_pk_value");
        headers.addAll(dataColumns);

        System.out.println("[buildCsvForRows] Headers: " + headers);

        // ── Lignes ───────────────────────────────────────────────────────────
        StringWriter sw = new StringWriter();
        PrintWriter  pw = new PrintWriter(sw);
        pw.println(buildCsvLine(headers));

        for (BscsErrorDetail errorDetail : rows) {
            List<String> values = new ArrayList<>();
            values.add(nvl(errorDetail.getTableName()));
            values.add(nvl(errorDetail.getRowPkValue()));

            String rawData = errorDetail.getFullRowData();
            System.out.println("[buildCsvForRows] RAW rowData=" + rawData);

            // Extraire toutes les valeurs de la ligne source
            Map<String, String> rowValues = extractAllValues(rawData, tableName, dataColumns);

            for (String col : dataColumns) {
                values.add(nvl(rowValues.get(col)));
            }

            pw.println(buildCsvLine(values));
        }
        pw.flush();

        // ── UTF-8 + BOM ──────────────────────────────────────────────────────
        return addBom(sw.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Construit un CSV pour des lignes de PLUSIEURS tables.
     * Chaque groupe de lignes par table a ses propres colonnes.
     * On génère un CSV avec table_name, _pk_value, puis les colonnes détectées.
     * Comme les tables ont des colonnes différentes, on utilise une union de toutes
     * les colonnes rencontrées (les cellules vides si une colonne n'existe pas pour une table).
     */
    private byte[] buildCsvForAllTables(List<BscsErrorDetail> rows) throws Exception {
        if (rows.isEmpty()) return null;

        // Regrouper par table
        Map<String, List<BscsErrorDetail>> byTable = new LinkedHashMap<>();
        for (BscsErrorDetail row : rows) {
            byTable.computeIfAbsent(row.getTableName(), k -> new ArrayList<>()).add(row);
        }

        // Déterminer toutes les colonnes (union) dans l'ordre d'apparition
        LinkedHashSet<String> allDataColumns = new LinkedHashSet<>();
        Map<String, List<String>> columnsPerTable = new LinkedHashMap<>();

        for (Map.Entry<String, List<BscsErrorDetail>> entry : byTable.entrySet()) {
            String tbl = entry.getKey();
            List<String> cols = getColumnsForTable(tbl, entry.getValue());
            columnsPerTable.put(tbl, cols);
            allDataColumns.addAll(cols);
        }

        List<String> headers = new ArrayList<>();
        headers.add("table_name");
        headers.add("_pk_value");
        headers.addAll(allDataColumns);

        System.out.println("[buildCsvForAllTables] Headers: " + headers);

        StringWriter sw = new StringWriter();
        PrintWriter  pw = new PrintWriter(sw);
        pw.println(buildCsvLine(headers));

        for (Map.Entry<String, List<BscsErrorDetail>> entry : byTable.entrySet()) {
            String tbl  = entry.getKey();
            List<String> tableCols = columnsPerTable.get(tbl);

            for (BscsErrorDetail errorDetail : entry.getValue()) {
                List<String> values = new ArrayList<>();
                values.add(nvl(errorDetail.getTableName()));
                values.add(nvl(errorDetail.getRowPkValue()));

                String rawData = errorDetail.getFullRowData();
                Map<String, String> rowValues = extractAllValues(rawData, tbl, tableCols);

                for (String col : allDataColumns) {
                    if (tableCols.contains(col)) {
                        values.add(nvl(rowValues.get(col)));
                    } else {
                        values.add(""); // colonne inexistante pour cette table
                    }
                }
                pw.println(buildCsvLine(values));
            }
        }
        pw.flush();

        return addBom(sw.toString().getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Retourne la liste ordonnée des colonnes pour une table donnée.
     * - Si la table est déclarée dans tableColumns → colonnes prédéfinies (tableau JSON)
     * - Sinon → colonnes extraites du premier full_row_data (objet JSON)
     */
    private List<String> getColumnsForTable(String tableName, List<BscsErrorDetail> rows) {
        if (tableName != null && tableColumns.containsKey(tableName.toUpperCase())) {
            return tableColumns.get(tableName.toUpperCase());
        }
        // Extraire les colonnes depuis les objets JSON des lignes
        LinkedHashSet<String> cols = new LinkedHashSet<>();
        for (BscsErrorDetail row : rows) {
            Map<String, Object> jsonObj = parseJsonObject(row.getFullRowData());
            if (!jsonObj.isEmpty()) {
                cols.addAll(jsonObj.keySet());
            }
        }
        return new ArrayList<>(cols);
    }

    /**
     * Extrait toutes les valeurs d'une ligne source (full_row_data)
     * sous forme de Map<colonne, valeur>.
     * Gère les deux formats : objet JSON et tableau JSON.
     */
    private Map<String, String> extractAllValues(String rawData, String tableName, List<String> columns) {
        Map<String, String> result = new LinkedHashMap<>();

        // Cas 1 : table déclarée → essayer d'abord objet JSON, puis tableau JSON
        if (tableName != null && tableColumns.containsKey(tableName.toUpperCase())) {
            List<String> predefinedCols = tableColumns.get(tableName.toUpperCase());

            // Tenter d'abord le parsing en objet JSON
            Map<String, Object> jsonObj = parseJsonObject(rawData);
            if (!jsonObj.isEmpty()) {
                for (String col : predefinedCols) {
                    Object val = jsonObj.get(col);
                    result.put(col, val != null ? val.toString() : "");
                }
                return result;
            }

            // Fallback : tableau JSON
            List<Object> array = parseJsonArray(rawData);
            for (int i = 0; i < predefinedCols.size(); i++) {
                String val = (i < array.size() && array.get(i) != null)
                        ? array.get(i).toString() : "";
                result.put(predefinedCols.get(i), val);
            }
            return result;
        }

        // Cas 2 : objet JSON (table non déclarée)
        Map<String, Object> jsonObj = parseJsonObject(rawData);
        if (!jsonObj.isEmpty()) {
            for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
                result.put(entry.getKey(),
                        entry.getValue() != null ? entry.getValue().toString() : "");
            }
            return result;
        }

        // Cas 3 : tableau JSON non déclaré
        List<Object> array = parseJsonArray(rawData);
        if (!array.isEmpty()) {
            for (int i = 0; i < columns.size() && i < array.size(); i++) {
                result.put(columns.get(i),
                        array.get(i) != null ? array.get(i).toString() : "");
            }
        }

        return result;
    }

    /**
     * Ajoute le BOM UTF-8 en tête du tableau d'octets.
     */
    private byte[] addBom(byte[] body) {
        byte[] bom = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};
        byte[] csv = new byte[bom.length + body.length];
        System.arraycopy(bom,  0, csv, 0,          bom.length);
        System.arraycopy(body, 0, csv, bom.length, body.length);
        return csv;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // POST : Import CSV corrigé
    // ─────────────────────────────────────────────────────────────────────────
    @PostMapping("/csv/import")
    public ResponseEntity<Map<String, Object>> importCorrectedCsv(
            @RequestParam("file") MultipartFile file,
            @RequestParam(required = false, defaultValue = "agent_migration") String correctedBy,
            @RequestParam(required = false) String comment
    ) {
        Map<String, Object> response = new LinkedHashMap<>();
        try {
            InputStream raw    = file.getInputStream();
            byte[]      first3 = raw.readNBytes(3);
            InputStream stream;
            if (first3.length == 3
                    && first3[0] == (byte) 0xEF
                    && first3[1] == (byte) 0xBB
                    && first3[2] == (byte) 0xBF) {
                stream = raw;
            } else {
                stream = new SequenceInputStream(new ByteArrayInputStream(first3), raw);
            }

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(stream, StandardCharsets.UTF_8));

            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.isBlank()) {
                response.put("success", false);
                response.put("message", "Fichier CSV vide ou sans en-tête.");
                return ResponseEntity.badRequest().body(response);
            }

            List<String> headers      = parseCsvLine(headerLine);
            int          tableNameIdx = headers.indexOf("table_name");
            int          pkValueIdx   = headers.indexOf("_pk_value");

            if (tableNameIdx == -1 || pkValueIdx == -1) {
                response.put("success", false);
                response.put("message", "Colonnes 'table_name' et '_pk_value' obligatoires.");
                return ResponseEntity.badRequest().body(response);
            }

            int updated = 0, notFound = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;
                List<String> values = parseCsvLine(line);
                if (values.size() < Math.max(tableNameIdx, pkValueIdx) + 1) continue;

                String tableName = values.get(tableNameIdx);
                String pkValue   = values.get(pkValueIdx);

                // Construire la map complète des données corrigées
                Map<String, Object> correctedData = new LinkedHashMap<>();
                for (int i = 0; i < headers.size(); i++) {
                    String h = headers.get(i);
                    if (!"table_name".equals(h) && !"_pk_value".equals(h)) {
                        correctedData.put(h, i < values.size() ? values.get(i) : "");
                    }
                }

                Optional<BscsErrorDetail> optRow =
                        repository.findByTableNameAndRowPkValue(tableName, pkValue);

                if (optRow.isPresent()) {
                    BscsErrorDetail detail        = optRow.get();
                    List<String>    predefinedCols = tableColumns.get(
                            tableName != null ? tableName.toUpperCase() : "");

                    if (predefinedCols != null) {
                        // Reconvertir en tableau JSON dans l'ordre des colonnes prédéfinies
                        List<String> arrayValues = new ArrayList<>();
                        for (String col : predefinedCols)
                            arrayValues.add(nvl((String) correctedData.get(col)));
                        detail.setCorrectedData(objectMapper.writeValueAsString(arrayValues));
                    } else {
                        detail.setCorrectedData(objectMapper.writeValueAsString(correctedData));
                    }

                    detail.setCorrectionStatus("CORRECTED");
                    detail.setCorrectedBy(correctedBy);
                    detail.setCorrectionComment(comment);
                    // CORRECTION : Utilisation de LocalDateTime.now() au lieu de Timestamp
                    detail.setCorrectionDate(LocalDateTime.now());
                    repository.save(detail);
                    updated++;
                } else {
                    notFound++;
                }
            }
            reader.close();

            response.put("success",  true);
            response.put("updated",  updated);
            response.put("notFound", notFound);
            response.put("message",  updated + " ligne(s) mise(s) à jour avec succès.");
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            e.printStackTrace();
            response.put("success", false);
            response.put("message", "Erreur import : " + e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Utilitaires privés
    // ─────────────────────────────────────────────────────────────────────────

    private String nvl(String s) { return s != null ? s : ""; }

    private String normalizeJson(String raw) {
        if (raw == null) return null;
        String s = raw.strip();
        if ((s.startsWith("\"{") && s.endsWith("}\""))
                || (s.startsWith("\"[") && s.endsWith("]\""))
                || (s.startsWith("\"") && s.endsWith("\""))) {
            s = s.substring(1, s.length() - 1)
                 .replace("\\\"", "\"")
                 .replace("\\\\", "\\");
        }
        int start = 0;
        while (start < s.length()) {
            char c = s.charAt(start);
            if (c == '{' || c == '[') break;
            start++;
        }
        return start < s.length() ? s.substring(start) : s;
    }

    private Map<String, Object> parseJsonObject(String raw) {
        try {
            String json = normalizeJson(raw);
            System.out.println("[parseJsonObject] normalized=" + json);
            if (json == null || !json.startsWith("{")) {
                System.out.println("[parseJsonObject] Pas un objet JSON.");
                return new LinkedHashMap<>();
            }
            Map<String, Object> result = objectMapper.readValue(json,
                    objectMapper.getTypeFactory()
                            .constructMapType(LinkedHashMap.class, String.class, Object.class));
            System.out.println("[parseJsonObject] OK clés=" + result.keySet());
            return result;
        } catch (Exception e) {
            System.out.println("[parseJsonObject] ERREUR: " + e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    private List<Object> parseJsonArray(String raw) {
        try {
            String json = normalizeJson(raw);
            if (json == null || !json.startsWith("[")) return Collections.emptyList();
            List<Object> result = objectMapper.readValue(json,
                    objectMapper.getTypeFactory()
                            .constructCollectionType(List.class, Object.class));
            System.out.println("[parseJsonArray] OK taille=" + result.size());
            return result;
        } catch (Exception e) {
            System.out.println("[parseJsonArray] ERREUR: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    private String buildCsvLine(List<String> fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(';');  
            String value = fields.get(i) == null ? "" : fields.get(i);
            value = value.replace("\"", "\"\"");
            sb.append('"').append(value).append('"');
        }
        return sb.toString();
    }

    private List<String> parseCsvLine(String line) {
        List<String>  fields   = new ArrayList<>();
        StringBuilder current  = new StringBuilder();
        boolean       inQuotes = false;
        int           i        = 0;

        while (i < line.length()) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i += 2;
                    } else {
                        inQuotes = false;
                        i++;
                    }
                } else {
                    current.append(c);
                    i++;
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                    i++;
                } else if (c == ';') {
                    fields.add(current.toString());
                    current.setLength(0);
                    i++;
                } else {
                    current.append(c);
                    i++;
                }
            }
        }
        fields.add(current.toString());
        return fields;
    }
}