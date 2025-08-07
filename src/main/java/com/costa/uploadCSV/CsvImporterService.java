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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class CsvImporterService {

    private final JdbcTemplate jdbcTemplate;
    private final MinioCsvService minioCsvService;

    private static final int THREAD_POOL_SIZE = 4;
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;
    private static final long LARGE_FILE_THRESHOLD = 500 * 1024 * 1024; // 500 MB
    private static final int BATCH_SIZE = 5000;
    private static final long MIN_ROW_COUNT_THRESHOLD = 1000; // Assume processed if table has >= 1000 rows

    public CsvImporterService(JdbcTemplate jdbcTemplate, MinioCsvService minioCsvService) {
        this.jdbcTemplate = jdbcTemplate;
        this.minioCsvService = minioCsvService;
    }

    private boolean isTableProcessed(String tableName, String objectName) {
        // Check if table exists
        String checkSql = "SELECT to_regclass(?)";
        String exists = jdbcTemplate.queryForObject(checkSql, new Object[]{tableName}, String.class);
        if (exists == null) {
            return false; // Table doesn't exist, not processed
        }

        // Check if table has significant rows
        String countSql = "SELECT COUNT(*) FROM \"" + tableName + "\"";
        Long rowCount = jdbcTemplate.queryForObject(countSql, Long.class);
        return rowCount != null && rowCount >= MIN_ROW_COUNT_THRESHOLD;
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
                        var stat = minioClient.statObject(
                                StatObjectArgs.builder().bucket(minioCsvService.getBucket()).object(objectName).build()
                        );
                        return new FileInfo(objectName, stat.size());
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
            String rawName = fileInfo.objectName.replaceFirst("(?i)\\.csv$", "");
            String tableName = rawName.toLowerCase().replaceAll("[^a-z0-9_]", "_");
            if (isTableProcessed(tableName, fileInfo.objectName)) {
                System.out.printf("[%s] Skipping processed file (table exists with >= %d rows)%n", fileInfo.objectName, MIN_ROW_COUNT_THRESHOLD);
                continue;
            }
            if (fileInfo.size > LARGE_FILE_THRESHOLD) {
                processFileWithRetries(fileInfo);
            } else {
                executor.submit(() -> processFileWithRetries(fileInfo));
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

    private void processFileWithRetries(FileInfo fileInfo) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                importSingleFile(fileInfo);
                break;
            } catch (Exception e) {
                attempt++;
                if (attempt == MAX_RETRIES) {
                    System.err.printf("[%s] Failed to import after %d attempts: %s%n", fileInfo.objectName, MAX_RETRIES, e.getMessage());
                } else {
                    System.err.printf("[%s] Retry %d: %s%n", fileInfo.objectName, attempt, e.getMessage());
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

    private void importSingleFile(FileInfo fileInfo) throws Exception {
        long start = System.currentTimeMillis();
        String objectName = fileInfo.objectName;
        String rawName = objectName.replaceFirst("(?i)\\.csv$", "");
        String tableName = rawName.toLowerCase().replaceAll("[^a-z0-9_]", "_");

        System.out.printf("[%s] Importing to Table: %s%n", objectName, tableName);

        try (InputStreamReader isr = new InputStreamReader(minioCsvService.getCsvFile(objectName), StandardCharsets.ISO_8859_1);
             BufferedReader reader = new BufferedReader(isr, 8192 * 4)) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                System.out.printf("[%s] Skipping empty file%n", objectName);
                return;
            }

            List<String> headers = Arrays.stream(headerLine.split(","))
                    .map(String::trim)
                    .map(h -> h.replaceAll("^\"|\"$", "").toLowerCase())
                    .filter(h -> !h.isEmpty())
                    .collect(Collectors.toList());
            System.out.printf("[%s] Cleaned headers (lowercase): %s%n", objectName, headers);

            if (headers.isEmpty()) {
                System.out.printf("[%s] Skipping file with invalid headers%n", objectName);
                return;
            }

            ensureTableExists(tableName, headers);

            String columnsList = headers.stream().map(h -> "\"" + h + "\"").collect(Collectors.joining(", "));
            String placeholders = headers.stream().map(h -> "?").collect(Collectors.joining(", "));
            String insertSQL = "INSERT INTO \"" + tableName + "\" (" + columnsList + ") VALUES (" + placeholders + ") ON CONFLICT DO NOTHING";

            try (Connection conn = jdbcTemplate.getDataSource().getConnection()) {
                conn.setAutoCommit(false);

                try (CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader(headers.toArray(new String[0])).withSkipHeaderRecord().withTrim().withAllowMissingColumnNames());
                     PreparedStatement insertStmt = conn.prepareStatement(insertSQL)) {

                    int inserted = 0, skipped = 0, batchCount = 0;
                    long lineNumber = 1; // Start after header

                    for (CSVRecord record : parser) {
                        lineNumber++;
                        // Check if record has the correct number of fields
                        if (record.size() < headers.size()) {
                            System.out.printf("[%s] ❌ Skipped line %d due to missing data: %s%n", objectName, lineNumber, record.toString());
                            skipped++;
                            continue;
                        }

                        try {
                            for (int i = 0; i < headers.size(); i++) {
                                String value = record.get(i);
                                // Clean invalid characters (e.g., 0x91)
                                if (value != null) {
                                    value = new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                                    value = value.replaceAll("[\\x00-\\x1F\\x7F\\x91]", "");
                                }
                                insertStmt.setString(i + 1, value);
                            }
                            insertStmt.addBatch();
                            batchCount++;

                            if (batchCount >= BATCH_SIZE) {
                                int[] results = insertStmt.executeBatch();
                                conn.commit();
                                inserted += Arrays.stream(results).sum();
                                System.out.printf("[%s] ✅ Batch executed: Size=%d, Total Inserted=%d, Skipped=%d%n", objectName, batchCount, inserted, skipped);
                                batchCount = 0;
                            }
                        } catch (Exception e) {
                            System.out.printf("[%s] ❌ Skipped line %d due to error: %s | Error: %s%n", objectName, lineNumber, record.toString(), e.getMessage());
                            skipped++;
                        }
                    }

                    if (batchCount > 0) {
                        int[] results = insertStmt.executeBatch();
                        conn.commit();
                        inserted += Arrays.stream(results).sum();
                        System.out.printf("[%s] ✅ Final Batch executed: Size=%d, Total Inserted=%d, Skipped=%d%n", objectName, batchCount, inserted, skipped);
                    }

                    long duration = System.currentTimeMillis() - start;
                    System.out.printf("[%s] Finished importing → Inserted: %d records, Skipped: %d records, Time: %d ms%n",
                            objectName, inserted, skipped, duration);

                } catch (Exception e) {
                    conn.rollback();
                    throw new Exception(String.format("[%s] Transaction rolled back: %s", objectName, e.getMessage()), e);
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
                System.out.printf("Created table: %s%n", tableName);
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