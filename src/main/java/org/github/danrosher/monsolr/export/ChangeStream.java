package org.github.danrosher.monsolr.export;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableMap;
import com.mongodb.MongoClient;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.github.danrosher.monsolr.exec.NamedPrefixThreadFactory;
import org.github.danrosher.monsolr.model.Broker;
import org.github.danrosher.monsolr.util.AppConfig;
import org.tomlj.TomlTable;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

@Log4j
public class ChangeStream extends Exporter implements Callable<Void> {

    private final ExecutorService exec = Executors.newCachedThreadPool(new NamedPrefixThreadFactory("changestream"));
    private final CountDownLatch done = new CountDownLatch(1);
    private final ConcurrentMap<String, Metrics> metrics = new ConcurrentHashMap<>();

    private Broker<SolrDocProcessor> broker;

    public ChangeStream(MongoClient client, SolrClient solrClient, AppConfig config) {
        super(client, config, solrClient);
    }

    @FunctionalInterface
    public interface CheckedFunction<T, U> {
        void apply(T t, U u) throws Exception;
    }

    @NoArgsConstructor
    public static final class Metrics {
        AtomicReference<BsonDocument> resumeTokenRef = new AtomicReference<>();
        AtomicLong cusorDelay = new AtomicLong();
        Meter requests = new Meter();
    }

    @AllArgsConstructor
    public static final class SolrDocProcessor {
        SolrInputDocument sdoc;
        CheckedFunction<UpdateRequest, SolrInputDocument> updateRequestFunction;
    }

    @Override
    public Void call() throws ExecutionException, InterruptedException {
        Runtime.getRuntime()
            .addShutdownHook(new Thread(ChangeStream.this::stop));
        broker = new Broker<>(config.getAppQSize());
        List<ScheduledFuture<?>> scheduledFutures = setupSchedulers();
        try {
            log.info("ChangeStream start:");
            List<Future<?>> tasks = new ArrayList<>();
            MongoDatabase database = client
                .getDatabase(Objects.requireNonNull(config.getMongoDB()));
            String mongo_collection = Objects.requireNonNull(config.getMongoCollection());
            String solr_collection = Objects.requireNonNull(config.getSolrCollection());
            addProducers(tasks, database, mongo_collection, solr_collection);
            int num_writers = config.getNumWriters();
            int writer_batch = config.getSolrWriterBatchSize();
            final String scoll = solr_collection.startsWith("$") ? solr_collection.substring(1) : solr_collection;
            final Function<SolrInputDocument, String> solr_collection_function = solr_collection.startsWith("$")
                ? (sdoc -> (String) sdoc.getFieldValue(scoll))
                : (x -> scoll);
            for (int i = 0; i < num_writers; i++) {
                tasks.add(exec.submit(() -> {
                    try {
                        Map<String, UpdateRequest> updateRequestMap = new HashMap<>();
                        int c = 0;
                        String solr_unique_key = config.getSolrUniqueKey();
                        AtomicBoolean export = new AtomicBoolean(false);
                        final int writer_delay = config.getSolrWriterDelay();
                        if (writer_delay > 0) {
                            Executors.newSingleThreadScheduledExecutor()
                                .scheduleAtFixedRate(() -> export.set(true), writer_delay, writer_delay, TimeUnit.SECONDS);
                        }
                        while (broker.isRunning()) {
                            SolrDocProcessor p = broker.take();
                            String coll = solr_collection_function.apply(p.sdoc);
                            if (coll != null && !"".equals(coll)) {
                                p.updateRequestFunction.apply(
                                    updateRequestMap.computeIfAbsent(coll, k -> new UpdateRequest()), p.sdoc);
                                c++;
                                if (c >= writer_batch) export.set(true);
                                if (export.get()) {
                                    for (Map.Entry<String, UpdateRequest> entry : updateRequestMap.entrySet())
                                        solrClient.request(entry.getValue(), entry.getKey());
                                    c = 0;
                                    updateRequestMap.clear();
                                    export.set(false);
                                }
                            } else {
                                log.warn(String.format("Unable to find collection for uniqueKey:%s",
                                    p.sdoc.getFieldValue(solr_unique_key)));
                            }
                        }
                    } catch (InterruptedException ignore) {
                    } catch (Exception e) {
                        log.error(exceptionToString(e));
                        throw new RuntimeException(e);
                    }
                }));
            }


            while (true) {
                boolean allComplete = true;
                for (Future<?> f : tasks) {
                    try {
                        f.get(1, TimeUnit.SECONDS);
                    } catch (TimeoutException ignore) {
                        allComplete = false;
                    }
                }
                if (allComplete)
                    break;
            }
        } catch (Exception e) {
            log.error(exceptionToString(e));
            broker.setRunning(false);
            throw e;
        } finally {
            log.info("ChangeStream finish:");
            done.countDown();
            scheduledFutures.forEach(f -> f.cancel(true));
            shutdownAndAwaitTermination(exec);
        }
        return null;
    }

    private List<ScheduledFuture<?>> setupSchedulers() {
        List<ScheduledFuture<?>> list = new ArrayList<>();
        int progress_delay = config.getAppProgressDelay();
        if (progress_delay > 0) {
            list.add(Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> metrics.forEach((name, metric) -> log.info(
                    String.format("[%s] m:%-3.2f 1m:%-3.2f 5m:%-3.2f 15m:%-3.2f delay:%d num:%d ", name,
                        metric.requests.getMeanRate(),
                        metric.requests.getOneMinuteRate(),
                        metric.requests.getFiveMinuteRate(),
                        metric.requests.getFifteenMinuteRate(),
                        metric.cusorDelay.get(),
                        metric.requests.getCount()))), progress_delay, progress_delay, TimeUnit.SECONDS));
        }
        int tracker_delay = config.getAppTrackerDelay();
        if (tracker_delay > 0) {
            final MongoCollection<Document> collection = client
                .getDatabase(Objects.requireNonNull(config.getMongoDB()))
                .getCollection(config.getMongoSyncCollection());
            list.add(Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> metrics.forEach((name, metric) -> trackMetrics(collection, name, metric)),
                    progress_delay, progress_delay, TimeUnit.SECONDS));
        }
        return list;
    }

    private void trackMetrics(MongoCollection<Document> collection, String name, Metrics metric) {
        collection.updateOne(eq("_id", name), set("resumeToken", metric.resumeTokenRef.get()), new UpdateOptions().upsert(true));
        collection.updateOne(eq("_id", name), set("delay", metric.cusorDelay.longValue()), new UpdateOptions().upsert(true));
    }

    private void addProducers(List<Future<?>> tasks, MongoDatabase database, String mongo_collection, final String solr_collection) {
        String id = config.getSolrUniqueKey();
        final String scoll = solr_collection.startsWith("$") ? solr_collection.substring(1) : solr_collection;
        final BiConsumer<SolrInputDocument, Document> solr_collection_function = solr_collection.startsWith("$")
            ? ((sdoc, doc) -> sdoc.setField(scoll, doc.getString(scoll)))
            : ((a, b) -> {
        }); //do nothing
        ImmutableMap.of(
            "create",
            ImmutableMap.of(
                "solrdocBuilder", (Function<ChangeStreamDocument<Document>, SolrInputDocument>)
                    d -> {
                        SolrInputDocument sdoc = new SolrInputDocument("_version_", "0");//overwrite
                        Objects.requireNonNull(d.getFullDocument())
                            .forEach(sdoc::setField);
                        solr_collection_function.accept(sdoc, d.getFullDocument());
                        return sdoc;
                    },
                "updateRequestFunction", (CheckedFunction<UpdateRequest, SolrInputDocument>)
                    UpdateRequest::add),

            "replace",
            ImmutableMap.of(
                "solrdocBuilder", (Function<ChangeStreamDocument<Document>, SolrInputDocument>)
                    d -> {
                        SolrInputDocument sdoc = new SolrInputDocument("_version_", "0");//overwtrite
                        Objects.requireNonNull(d.getFullDocument())
                            .forEach(sdoc::setField);
                        solr_collection_function.accept(sdoc, d.getFullDocument());
                        return sdoc;
                    },
                "updateRequestFunction", (CheckedFunction<UpdateRequest, SolrInputDocument>)
                    UpdateRequest::add),

            "update",
            ImmutableMap.of(
                "solrdocBuilder", (Function<ChangeStreamDocument<Document>, SolrInputDocument>)
                    d -> {
                        SolrInputDocument sdoc = new SolrInputDocument(
                            "_version_", "1",
                            id, Objects.requireNonNull(d.getFullDocument())
                            .getString(id));//doc must exist
                        if (d.getUpdateDescription() != null && d.getUpdateDescription()
                            .getUpdatedFields() != null)
                            new DocumentCodec().decode(new BsonDocumentReader(d.getUpdateDescription()
                                .getUpdatedFields()), DecoderContext.builder()
                                .build())
                                .forEach((key, value) -> sdoc.setField(key, ImmutableMap.of("set", value == null ? "null" : value)));
                        if (d.getUpdateDescription() != null && d.getUpdateDescription()
                            .getRemovedFields() != null)
                            d.getUpdateDescription()
                                .getRemovedFields()
                                .forEach((k) -> sdoc.setField(k, ImmutableMap.of("set", "null")));
                        solr_collection_function.accept(sdoc, d.getFullDocument());
                        return sdoc;
                    },
                "updateRequestFunction", (CheckedFunction<UpdateRequest, SolrInputDocument>)
                    UpdateRequest::add),

            "delete",
            ImmutableMap.of(
                "solrdocBuilder", (Function<ChangeStreamDocument<Document>, SolrInputDocument>)
                    d -> {
                        SolrInputDocument sdoc = new SolrInputDocument(id, Objects.requireNonNull(d.getFullDocument())
                            .getString(id));
                        solr_collection_function.accept(sdoc, d.getFullDocument());
                        return sdoc;
                    },
                "updateRequestFunction", (CheckedFunction<UpdateRequest, SolrInputDocument>)
                    (u, s) -> u.deleteById((String.valueOf(s.getFieldValue(id)))))
        )

            .forEach((String op, ImmutableMap map) -> {
                for (final Object o : config.getChangeStream(op)
                    .toList()) {
                    TomlTable t = (TomlTable) o;
                    Function<ChangeStreamDocument<Document>, SolrInputDocument> solrdocBuilder = (Function<ChangeStreamDocument<Document>, SolrInputDocument>) map.get("solrdocBuilder");
                    CheckedFunction<UpdateRequest, SolrInputDocument> updateRequestFunction = (CheckedFunction<UpdateRequest, SolrInputDocument>) map.get("updateRequestFunction");
                    String name = t.getString("name");
                    ChangeStreamIterable<Document> stream = getStream(database, mongo_collection, t.getString("mongo-pipeline"));
                    int batchSize = config.getMongoBatchSize();
                    if (batchSize > 0) {
                        stream.batchSize(batchSize);
                    }
                    int epochStart = config.getMongoStartEpoch();
                    if (epochStart > 0) {
                        log.info(String.format("[%s] Starting from epoch in config: %d", name, epochStart));
                        stream.startAtOperationTime(new BsonTimestamp(epochStart, 0));
                    } else {
                        BsonDocument resumeToken = getResumeToken(name, database);
                        if (resumeToken != null) {
                            log.info(String.format("[%s] Starting from resumeToken: %s", name, resumeToken));
                            stream.resumeAfter(resumeToken);
                        } else {
                            log.info(String.format("[%s] No resumeToken so starting from NOW", name));
                        }
                    }
                    addProducer(name, tasks, stream.iterator(), solrdocBuilder, updateRequestFunction);
                }
            });
    }

    private BsonDocument getResumeToken(String name, MongoDatabase database) {
        BsonDocument resumeToken = null;
        MongoCollection<Document> collection = database.getCollection(config.getMongoSyncCollection());
        Document d = collection.find(eq("_id", name))
            .first();
        if (d != null) {
            Document resumeDocument = (Document) d.get("resumeToken");
            if (resumeDocument != null) {
                resumeToken = resumeDocument.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
            }
        }
        return resumeToken;
    }

    private void addProducer(String name, List<Future<?>> tasks, MongoCursor<ChangeStreamDocument<Document>> cursor,
                             Function<ChangeStreamDocument<Document>, SolrInputDocument> solrdocBuilder, CheckedFunction<UpdateRequest,
        SolrInputDocument> updateRequestFunction) {
        final Metrics metric = new Metrics();
        metrics.put(name, metric);
        tasks.add(exec.submit(() -> {
            try {
                while (broker.isRunning()) {
                    ChangeStreamDocument<Document> d = cursor.tryNext();
                    if (d != null) {
                        metric.requests.mark();
                        broker.put(
                            new SolrDocProcessor(solrdocBuilder.apply(d),
                                updateRequestFunction));
                        if (d.getClusterTime() != null) {
                            metric.cusorDelay.set(Instant.now(Clock.systemDefaultZone())
                                .getEpochSecond() - d.getClusterTime()
                                .getTime());
                        }
                        metric.resumeTokenRef.set(d.getResumeToken());
                    } else {
                        metric.cusorDelay.set(0);
                    }
                }

            } catch (InterruptedException ignore) {
                log.info("Producer interrupted");
            } finally {
                broker.setRunning(false);
                done.countDown();
                shutdownAndAwaitTermination(exec);
            }
            log.info("producer done");
        }));
    }

    private void stop() {
        try {
            broker.setRunning(false);
            done.await();
        } catch (InterruptedException ignore) {
        }
        shutdownAndAwaitTermination(exec);
    }
}