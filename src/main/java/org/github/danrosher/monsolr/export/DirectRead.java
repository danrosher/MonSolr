package org.github.danrosher.monsolr.export;

import com.codahale.metrics.Meter;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.bson.Document;
import org.github.danrosher.monsolr.exec.NamedPrefixThreadFactory;
import org.tomlj.TomlParseResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.github.danrosher.monsolr.util.Util.getOption;

@Log4j
public class DirectRead extends Exporter implements Callable<Void> {

    private static final Document POISON = new Document();

    private final ExecutorService exec = Executors.newCachedThreadPool(new NamedPrefixThreadFactory("directread"));
    private final CountDownLatch done = new CountDownLatch(1);
    private Future<?> producerFuture;
    private final Meter requests = new Meter();

    public DirectRead(MongoClient client, SolrClient solrClient, TomlParseResult config) {
        super(client, config, solrClient);
    }

    @Override
    public Void call() throws Exception {
        List<ScheduledFuture<?>> scheduledFutures = setupSchedulers();
        try {
            Runtime.getRuntime()
                .addShutdownHook(new Thread(DirectRead.this::stop));
            MongoCollection<Document> collection = client
                .getDatabase(Objects.requireNonNull(config.getString("mongo-db")))
                .getCollection(Objects.requireNonNull(config.getString("mongo-collection")));
            AggregateIterable<Document> iterable = getIterable(collection,
                config.getString("mongo-pipeline"),
                getOption(config, "mongo-batchsize", 0)
            );
            int num_writers = getOption(config, "solr-num-writers", 1);
            int writer_batch = getOption(config, "solr-writer-batch", 1000);
            int queue_size = getOption(config, "app-queue-size", num_writers * writer_batch);
            ArrayBlockingQueue<Document> queue = new ArrayBlockingQueue<>(queue_size, true);
            producerFuture = exec.submit(() -> {
                log.debug("Producer start");
                try {
                    for (Document d : iterable) {
                        requests.mark();
                        queue.put(d);
                    }
                } catch (InterruptedException ignore) {
                } finally {
                    for (int i = 0; i < num_writers; i++) {
                        try {
                            queue.put(POISON);
                        } catch (InterruptedException ignore) {
                        }
                    }
                    log.debug("Producer finish");
                }
            });
            List<Future<?>> tasks = new ArrayList<>(num_writers);
            String solr_collection = Objects.requireNonNull(config.getString("solr-collection"));
            Function<SolrInputDocument, String> solr_collection_function = (x -> solr_collection);
            if (solr_collection.startsWith("$")) {
                final String solr_collection_field = solr_collection.substring(1);
                solr_collection_function = (sdoc -> (String) sdoc.getFieldValue(solr_collection_field));
            }
            for (int i = 0; i < num_writers; i++) {
                Function<SolrInputDocument, String> finalSolr_collection_function = solr_collection_function;
                tasks.add(exec.submit(() -> {
                    try {
                        Map<String, UpdateRequest> updateRequestMap = new HashMap<>();
                        int c = 0;
                        while (true) {
                            Document d = queue.take();
                            if (d == POISON)
                                break;
                            SolrInputDocument solrDoc = new SolrInputDocument();
                            for (Map.Entry<String, Object> e : d.entrySet()) solrDoc.setField(e.getKey(), e.getValue());
                            String coll = finalSolr_collection_function.apply(solrDoc);
                            if (coll != null && !"".equals(coll)) {
                                updateRequestMap.computeIfAbsent(coll, k -> new UpdateRequest())
                                    .add(solrDoc);
                                c++;
                                if (c >= writer_batch) {
                                    for (Map.Entry<String, UpdateRequest> entry : updateRequestMap.entrySet())
                                        solrClient.request(entry.getValue(), entry.getKey());
                                    c = 0;
                                    updateRequestMap.clear();
                                }
                            }
                        }
                    } catch (IOException | SolrServerException e) {
                        log.error(e);
                        throw new RuntimeException(e);
                    } catch (InterruptedException ignore) {
                    }
                }));
            }

            while (true) {
                boolean allComplete = true;
                for (Future<?> f : tasks) {
                    try {
                        f.get(100, TimeUnit.MILLISECONDS);
                    } catch (TimeoutException ignore) {
                        allComplete = false;
                    }
                }
                if (allComplete)
                    break;
            }
        } catch (Exception e) {
            log.error(e.getLocalizedMessage());
            if (producerFuture != null)
                producerFuture.cancel(true);
            throw e;
        } finally {
            done.countDown();
            scheduledFutures.forEach(f -> f.cancel(true));
            shutdownAndAwaitTermination(exec);
        }

        log.info(String.format("Fetched %d documents", requests.getCount()));
        return null;
    }

    private List<ScheduledFuture<?>> setupSchedulers() {
        List<ScheduledFuture<?>> list = new ArrayList<>();
        int progress_delay = getInt("app-progress-delay-secs", 0);
        if (progress_delay > 0) {
            list.add(Executors.newSingleThreadScheduledExecutor()
                .scheduleAtFixedRate(() -> log.info(
                    String.format("m:%-3.2f 1m:%-3.2f 5m:%-3.2f 15m:%-3.2f num:%d ",
                        requests.getMeanRate(), requests.getOneMinuteRate(), requests.getFiveMinuteRate(),
                        requests.getFifteenMinuteRate(), requests.getCount())), progress_delay, progress_delay,
                    TimeUnit.SECONDS));
        }
        return list;
    }

    public void stop() {
        if (producerFuture != null)
            producerFuture.cancel(true);
        try {
            done.await();
        } catch (InterruptedException ignore) {
        }
        shutdownAndAwaitTermination(exec); }
}
