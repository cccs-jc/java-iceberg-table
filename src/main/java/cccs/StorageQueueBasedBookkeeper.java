package cccs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.ParquetUtil;
import org.json.JSONObject;

import com.azure.core.util.Context;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.QueueClientBuilder;
import com.azure.storage.queue.models.QueueMessageItem;


public class StorageQueueBasedBookkeeper {
    
    private ExecutorService executorService = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    private QueueClient queueClient;
    private long runningCountAppendFile = 0;
    private final Pattern blobUrlPattern = Pattern.compile("https:\\/\\/([^\\/]*)\\/([^\\/]*)\\/(.*)");
    private long cumulativeLatency;
    private Table table;

    private class Moniker {
        public final String filePath;
        public final long fileSize;

        public Moniker(String filePath, long fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }
    }

    public StorageQueueBasedBookkeeper() {
        this.table = Constants.tables.load(Constants.tableLocation);
    }

    public Metrics readParquetMetrics(String f) {
        InputFile in = this.table.io().newInputFile(f);
        return ParquetUtil.fileMetrics(in, MetricsConfig.forTable(this.table));
    }

    /*
   * Sample message from Event Grid
   * <!-- @formatter:off -->
   *    <code>
    *   {
    *    "topic": "/subscriptions/xxxxx-xxxx-xxxx-xxxx-xxxxxx/resourceGroups/<groupname>/providers/Microsoft.Storage/storageAccounts/<accountname>",
    *    "subject": "/blobServices/default/containers/users/blobs/iceberg/schema/data/file.parquet",
    *    "eventType": "Microsoft.Storage.BlobCreated",
    *    "id": "d240ae8e-c01e-005e-0fc5-13945206eae4",
    *    "data": {
    *        "api": "PutBlob",
    *        "clientRequestId": "4f2b333c-eb82-4cc4-aafd-9d9e8bc07020",
    *        "requestId": "d240ae8e-c01e-005e-0fc5-139452000000",
    *        "eTag": "0x8DAE1DC9D7D9D30",
    *        "contentType": "application/x-compressed",
    *        "contentLength": 115739,
    *        "blobType": "BlockBlob",
    *        "blobUrl": "https://<accountname>.blob.core.windows.net/users/iceberg/schema/data/file.parquet",
    *        "url": "https://<accountname>.blob.core.windows.net/users/iceberg/schema/data/file.parquet",
    *        "sequencer": "0000000000000000000000000001520500000000000a67be",
    *        "identity": "$superuser",
    *        "storageDiagnostics": {
    *            "batchId": "9ed52c2e-d006-000f-00c5-1309de000000"
    *        }
    *    },
    *    "dataVersion": "",
    *    "metadataVersion": "1",
    *    "eventTime": "2022-12-19T16:18:07.7659358Z"
    *}
    *</code>
    * <!-- @formatter:on -->
   */

    public QueueClient getOrCreateQueueClient() {
        if (this.queueClient == null) {
            DataLakeTokenCredential token = new DataLakeTokenCredential();
            String queueURL = Constants.hadoopConf.get("queue.location");
            String queueName = Constants.hadoopConf.get("queue.name");
            this.queueClient = new QueueClientBuilder()
                    .endpoint(queueURL)
                    .credential(token)
                    .queueName(queueName)
                    .buildClient();
        }
        return this.queueClient;
    }

    public List<Moniker> parseMessageBatch(List<QueueMessageItem> messages) {
        List<Moniker> monikers = new ArrayList<Moniker>();
        for (QueueMessageItem message : messages) {
            // Do processing for all messages in less than 5 minutes,
            Moniker m = this.parseMessage(message);
            if (m != null) {
                monikers.add(m);
            }
        }
        return monikers;
    }

    public class MetricResolver implements Callable<DataFile> {
        private final Moniker moniker;
        private PartitionSpec partitionSpec;

        public MetricResolver(Moniker moniker, PartitionSpec partitionSpec) {
            this.moniker = moniker;
            this.partitionSpec = partitionSpec;
        }

        public DataFile call() throws Exception {
            String format = null;
            Metrics metrics = null;
            if (moniker.filePath.endsWith(".parquet")) {
                metrics = readParquetMetrics(moniker.filePath);
                format = "PARQUET";
            } else if (moniker.filePath.endsWith(".avro")) {
                metrics = readParquetMetrics(moniker.filePath);
                format = "AVRO";
            } else {
                return null;
            }

            DataFile dataFile = DataFiles.builder(this.partitionSpec)
                    .withPath(moniker.filePath)
                    .withFileSizeInBytes(moniker.fileSize)
                    .withFormat(format)
                    .withMetrics(metrics)
                    .build();
            return dataFile;
        }
    }

    public FileSystem getFileSystem() {
        return Util.getFs(Constants.tableLocationPath, Constants.hadoopConf);
    }

    public List<DataFile> resolveMetrics(List<Moniker> monikers) throws Exception {
        List<Callable<DataFile>> callableTasks = monikers.stream()
                .map(m -> new MetricResolver(m, Constants.partitionSpec))
                .collect(Collectors.toList());

        List<Future<DataFile>> futures = executorService.invokeAll(callableTasks);
        return futures.stream().map(f -> {
            try {
                return f.get();
            } catch (Exception e) {
                return null;
            }
        }).collect(Collectors.toList());
    }

    public Moniker parseMessage(QueueMessageItem message) {
        Moniker m = null;
        try {
            String jsonString = new String(Base64.getDecoder().decode(message.getBody().toString()));
            JSONObject json = new JSONObject(jsonString);
            String eventType = json.getString("eventType");
            boolean isBlobCreated = "Microsoft.Storage.BlobCreated".equals(eventType);
            if (isBlobCreated) {
                String blobUrl = json.getJSONObject("data").getString("blobUrl");
                String api = json.getJSONObject("data").getString("api");

                boolean isFlush = "FlushWithClose".equals(api);
                boolean inData = blobUrl.contains("/data/");
                boolean isParquet = blobUrl.endsWith(".parquet");
                boolean isAvro = blobUrl.endsWith(".avro");

                if (isFlush && inData && (isParquet || isAvro)) {
                    // System.out.println(jsonString);
                    long contentLength = json.getJSONObject("data").getLong("contentLength");
                    Matcher matcher = blobUrlPattern.matcher(blobUrl);
                    if (matcher.matches()) {
                        String host = matcher.group(1);
                        String container = matcher.group(2);
                        String path = matcher.group(3);
                        String filePath = "abfss://" + container + "@" + host + "/" + path;
                        // System.out.println(filePath);
                        m = new Moniker(filePath, contentLength);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return m;
    }

    public long processStorageQueue() {
        try {
            long start = System.currentTimeMillis();
            QueueClient queueClient = this.getOrCreateQueueClient();

            // Retrieve 20 messages from the queue with a
            // visibility timeout of 60 seconds (5 minutes)
            int MAX_MESSAGES = 32;
            List<QueueMessageItem> messagesToDelete = new ArrayList<QueueMessageItem>();
            List<Moniker> monikers = new ArrayList<Moniker>();

            List<QueueMessageItem> messages = queueClient.receiveMessages(MAX_MESSAGES,
                    Duration.ofSeconds(60), Duration.ofSeconds(10), new Context("key1", "value1")).stream()
                    .collect(Collectors.toList());

            if (messages.size() > 0) {
                long startParseMessages = System.currentTimeMillis();
                while (messages.size() > 0 && monikers.size() < 500) {
                    monikers.addAll(parseMessageBatch(messages));
                    messagesToDelete.addAll(messages);
                    // process next batch
                    messages = queueClient.receiveMessages(MAX_MESSAGES,
                            Duration.ofSeconds(60), Duration.ofSeconds(10), new Context("key1", "value1")).stream()
                            .collect(Collectors.toList());
                }
                long endParseMessages = System.currentTimeMillis();
                System.out.println("parse " + (endParseMessages - startParseMessages));

                if (monikers.size() > 0) {
                    long now = System.currentTimeMillis();
                    FileSystem fs = getFileSystem();
                    for (Moniker m1 : monikers) {
                        FileStatus status = fs.getFileStatus(new Path(m1.filePath));
                        long mod = status.getModificationTime();
                        long latency = (now - mod);
                        cumulativeLatency += latency;
                    }

                    long startReadMetrics = System.currentTimeMillis();
                    List<DataFile> dataFiles = this.resolveMetrics(monikers);
                    long endReadMetrics = System.currentTimeMillis();
                    System.out.println("metrics " + (endReadMetrics - startReadMetrics));

                    AppendFiles append = this.table.newAppend();
                    for (DataFile dataFile : dataFiles) {
                        append.appendFile(dataFile);
                        runningCountAppendFile++;
                    }

                    long startAppend = System.currentTimeMillis();
                    append.commit();
                    long endAppend = System.currentTimeMillis();
                    System.out.println("append " + (endAppend - startAppend));
                }

                long startDeleteFromQueue = System.currentTimeMillis();
                for (QueueMessageItem delMessage : messagesToDelete) {
                    // deleting each message after processing.
                    queueClient.deleteMessage(delMessage.getMessageId(), delMessage.getPopReceipt());
                }
                long endDeleteFromQueue = System.currentTimeMillis();
                System.out.println("delete " + (endDeleteFromQueue - startDeleteFromQueue));
            }
            long end = System.currentTimeMillis();
            if (monikers.size() > 0) {
                System.out.println(
                        "Total time append " + monikers.size() + " files (ms): "
                                + (end - start) + " Total appended files: " + runningCountAppendFile
                                + " Cumulative latency: " + cumulativeLatency + " Avg latency: "
                                + cumulativeLatency / runningCountAppendFile);
            }

            return (end - start);
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    public long markOldDataFileForDelete(int retentionMs) {
        long start = System.currentTimeMillis();
        long deleteMs = (System.currentTimeMillis() - retentionMs);
        long deleteMicros = deleteMs * 1000;
        deleteMicros = deleteMicros - (deleteMicros % Constants.widthMicros);
        System.out.println("deleting data files older than: " + deleteMicros);
        this.table.newDelete().deleteFromRowFilter(Expressions.lessThan("timeperiod_loadedBy", deleteMicros)).commit();
        long end = System.currentTimeMillis();
        System.out.println("Time to delete old data (ms): " + (end - start));
        return (end - start);
      }
    
}