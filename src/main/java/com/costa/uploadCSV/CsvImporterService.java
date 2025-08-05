package com.costa.uploadCSV;

import jakarta.annotation.PostConstruct;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import io.minio.MinioClient;
import io.minio.ListObjectsArgs;
import io.minio.StatObjectArgs;
import io.minio.Result;
import io.minio.messages.Item;
import okhttp3.OkHttpClient;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class CsvImporterService {

    private final JdbcTemplate jdbcTemplate;
    private final MinioCsvService minioCsvService;

    private static final int THREAD_POOL_SIZE = 4;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;
    private static final long LARGE_FILE_THRESHOLD = 500 * 1024 * 1024;
    private static final int BATCH_SIZE = 5000;

    public CsvImporterService(JdbcTemplate jdbcTemplate, MinioCsvService minioCsvService) {
        this.jdbcTemplate = jdbcTemplate;
        this.minioCsvService = minioCsvService;
    }

    @PostConstruct
    public void importAllCsvFiles() throws Exception {
        long apiStartTime = System.currentTimeMillis();
        System.out.println("CSV Import API started at: " + new Date(apiStartTime));

        OkHttpClient httpClient = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(60))
                .readTimeout(Duration.ofMinutes(5))
                .writeTimeout(Duration.ofMinutes(5))
                .build();

        MinioClient minioClient = MinioClient.builder()
                .endpoint(minioCsvService.getMinioUrl())
                .credentials(minioCsvService.getAccessKey(), minioCsvService.getSecretKey())
                .httpClient(httpClient)
                .build();

        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket(minioCsvService.getBucket()).build()
        );

        List<FileInfo> csvFiles = StreamSupport.stream(results.spliterator(), false)
                .map(result -> {
                    try {
                        Item item = result.get();
                        String objectName = item.objectName();
                        if (!objectName.toLowerCase().endsWith(".csv")) return null;
                        long size = minioClient.statObject(
                                StatObjectArgs.builder().bucket(minioCsvService.getBucket()).object(objectName).build()
                        ).size();
                        return new FileInfo(objectName, size);
                    } catch (Exception e) {
                        System.err.println("Error accessing object: " + e.getMessage());
                        return null;
                    }
                })
                .filter(fileInfo -> fileInfo != null)
                .collect(Collectors.toList());

        if (csvFiles.isEmpty()) {
            System.out.println("No CSV files found in MinIO bucket: " + minioCsvService.getBucket());
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        for (FileInfo fileInfo : csvFiles) {
            if (fileInfo.size > LARGE_FILE_THRESHOLD) {
                processFileWithRetries(fileInfo.objectName);
            } else {
                executor.submit(() -> processFileWithRetries(fileInfo.objectName));
            }
        }

        executor.shutdown();
        if (!executor.awaitTermination(120, TimeUnit.MINUTES)) {
            System.err.println("Some tasks did not complete within the timeout.");
            executor.shutdownNow();
        }

        long apiEndTime = System.currentTimeMillis();
        long totalTime = apiEndTime - apiStartTime;

        System.out.println("CSV Import API ended at: " + new Date(apiEndTime));
        System.out.println("Total time taken: " + totalTime + " ms");
    }

    private void processFileWithRetries(String objectName) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                importSingleFile(objectName);
                break;
            } catch (Exception e) {
                attempt++;
                if (attempt == MAX_RETRIES) {
                    System.err.println("Failed to import " + objectName + " after " + MAX_RETRIES + " attempts: " + e.getMessage());
                } else {
                    System.err.printf("Retry %d for %s: %s%n", attempt, objectName, e.getMessage());
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
    }

    private void importSingleFile(String objectName) throws Exception {
        long start = System.currentTimeMillis();
        String rawName = objectName.replaceFirst("(?i)\\.csv$", "");
        String tableName = rawName.toLowerCase().replaceAll("[^a-z0-9_]", "_");

        System.out.println("\nImporting: " + objectName + " → Table: " + tableName);

        try (InputStreamReader isr = new InputStreamReader(minioCsvService.getCsvFile(objectName), StandardCharsets.ISO_8859_1);
             BufferedReader reader = new BufferedReader(isr, 8192 * 4)) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                System.out.println("Skipping empty file: " + objectName);
                return;
            }

            List<String> headers = Arrays.stream(headerLine.split(","))
                    .map(String::trim)
                    .map(h -> h.replaceAll("^\"|\"$", "").toLowerCase())
                    .filter(h -> !h.isEmpty())
                    .collect(Collectors.toList());
            System.out.println("Cleaned headers (lowercase): " + headers);

            if (headers.isEmpty()) {
                System.out.println("Skipping file with invalid headers: " + objectName);
                return;
            }

            ensureTableExists(tableName, headers);

            String columnsList = headers.stream().map(h -> "\"" + h + "\"").collect(Collectors.joining(", "));
            String placeholders = headers.stream().map(h -> "?").collect(Collectors.joining(", "));
            String checkWhereClause = headers.stream().map(h -> "\"" + h + "\" = ?").collect(Collectors.joining(" AND "));

            String selectSQL = "SELECT COUNT(*) FROM \"" + tableName + "\" WHERE " + checkWhereClause;
            String insertSQL = "INSERT INTO \"" + tableName + "\" (" + columnsList + ") VALUES (" + placeholders + ")";

            try (Connection conn = jdbcTemplate.getDataSource().getConnection()) {
                conn.setAutoCommit(false);

                try (CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).withSkipHeaderRecord().withTrim());
                     PreparedStatement checkStmt = conn.prepareStatement(selectSQL);
                     PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {

                    int inserted = 0, skipped = 0, batchCount = 0;

                    for (CSVRecord record : parser) {
                        for (int i = 0; i < headers.size(); i++) {
                            checkStmt.setString(i + 1, record.get(i));
                        }

                        try (ResultSet rs = checkStmt.executeQuery()) {
                            rs.next();
                            if (rs.getInt(1) == 0) {
                                for (int i = 0; i < headers.size(); i++) {
                                    insertStmt.setString(i + 1, record.get(i));
                                }
                                insertStmt.addBatch();
                                batchCount++;

                                if (batchCount >= BATCH_SIZE) {
                                    insertStmt.executeBatch();
                                    conn.commit();
                                    inserted += batchCount;
                                    System.out.printf("✅ Batch executed: Size=%d, Total Inserted=%d%n", BATCH_SIZE, inserted);
                                    batchCount = 0;
                                }
                            } else {
                                skipped++;
                            }
                        }
                    }

                    if (batchCount > 0) {
                        insertStmt.executeBatch();
                        conn.commit();
                        inserted += batchCount;
                        System.out.printf("✅ Final Batch executed: Size=%d, Total Inserted=%d%n", batchCount, inserted);
                    }

                    long duration = System.currentTimeMillis() - start;
                    System.out.printf("Finished importing %s → Inserted: %d records | Skipped: %d records | Time: %d ms%n",
                            objectName, inserted, skipped, duration);

                } catch (Exception e) {
                    conn.rollback();
                    throw new Exception("Transaction rolled back for " + objectName + ": " + e.getMessage(), e);
                } finally {
                    conn.setAutoCommit(true);
                }
            }
        }
    }

    private void ensureTableExists(String tableName, List<String> headers) {
        synchronized (CsvImporterService.class) {
            String checkSql = "SELECT to_regclass('" + tableName + "')";
            String exists = jdbcTemplate.queryForObject(checkSql, String.class);

            if (exists == null) {
                String createSql = "CREATE TABLE \"" + tableName + "\" (" +
                        headers.stream().map(h -> "\"" + h + "\" TEXT").collect(Collectors.joining(", ")) +
                        ")";
                jdbcTemplate.execute(createSql);
                System.out.println("Created table: " + tableName);
            }
        }
    }

    private static class FileInfo {
        String objectName;
        long size;

        FileInfo(String objectName, long size) {
            this.objectName = objectName;
            this.size = size;
        }
    }
}
