package org.github.danrosher.monsolr.export;

import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.FullDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrClient;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.json.JsonReader;
import org.github.danrosher.monsolr.util.AppConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Log4j
public abstract class Exporter {

    protected final MongoClient client;
    protected final AppConfig config;
    protected final SolrClient solrClient;

    private static final BsonArrayCodec arrayReader = new BsonArrayCodec(CodecRegistries.fromProviders(List.of(new ValueCodecProvider(), new BsonValueCodecProvider(), new DocumentCodecProvider())));

    List<BsonDocument> getAggregates(String json){
        JsonReader reader = new JsonReader(json);
        return arrayReader.decode(reader, DecoderContext.builder()
            .build())
            .stream()
            .map(BsonValue::asDocument)
            .collect(Collectors.toList());
    }

    AggregateIterable<Document> getIterable(MongoCollection<Document> collection, String json, int batchSize) {
        List<BsonDocument> aggregates = getAggregates(json);
        final AggregateIterable<Document> iterable = collection.aggregate(aggregates);
        if (batchSize > 0) iterable.batchSize(batchSize);
        return iterable;
    }

    ChangeStreamIterable<Document>  getStream(MongoDatabase database, String collection, String json){
        List<BsonDocument> aggregates = getAggregates(json);
        return database.getCollection(collection).watch(aggregates)
            .fullDocument(FullDocument.UPDATE_LOOKUP);
    }

    static void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            long SHUTDOWN_TIMEOUT = 1;
            if (!pool.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.SECONDS))
                    log.error("Pool did not terminate");
            }

        } catch (InterruptedException ie) {
            log.error("shutdown interrupted");
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    public String exceptionToString(Exception ex){
        StringWriter errors = new StringWriter();
        ex.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
}
