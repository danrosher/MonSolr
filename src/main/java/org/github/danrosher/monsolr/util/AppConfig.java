package org.github.danrosher.monsolr.util;

import lombok.Data;
import org.tomlj.TomlArray;
import org.tomlj.TomlParseResult;

import java.util.Objects;

import static org.github.danrosher.monsolr.util.Util.getOption;
import static org.github.danrosher.monsolr.util.Util.getRequired;

@Data
public class AppConfig {

    private final String solrCollection;
    private final int solrWriterDelay;
    private final String mongoURL;
    private final String solrURL;
    private final int numWriters;
    private final boolean isDirect;
    private final boolean isChangeStream;
    private final String mongoDB;
    private final String mongoCollection;
    private final String mongoPipeline;
    private final int mongoBatchSize;
    private final int solrWriterBatchSize;
    private final String solrUniqueKey;
    private final int appQSize;
    private final int appProgressDelay;
    private final int appTrackerDelay;
    private final String mongoSyncCollection;
    private final int mongoStartEpoch;

    private TomlParseResult _config;

    public AppConfig(TomlParseResult config) throws Throwable {

        _config = config;

        //required
        mongoURL = Objects.requireNonNull(config.getString("mongo-url"));
        solrURL = getRequired(config, "solr-url");
        solrUniqueKey = getRequired(config, "solr-unique-key");
        solrCollection = getRequired(config, "solr-collection");
        mongoDB = getRequired(config,"mongo-db");
        mongoCollection = getRequired(config,"mongo-collection");

        //mongo - optional
        mongoPipeline = config.getString("mongo-pipeline");
        mongoBatchSize = getOption(config, "mongo-batchsize", 0);
        mongoSyncCollection = getOption(config,"sync-collection", "monsolr");
        mongoStartEpoch = getOption(config,"mongo-start-epoch", 0);

        //solr - optional
        solrWriterBatchSize = getOption(config, "solr-writer-batch", 1000);
        solrWriterDelay = getOption(config,"solr-writer-delay", -1);

        //app
        numWriters = getOption(config, "solr-num-writers", 1);
        isDirect = config.contains("mongo-pipeline");
        isChangeStream = config.contains("changestream");
        appQSize = getOption(config, "app-queue-size", numWriters * solrWriterBatchSize);
        appProgressDelay = getOption(config,"app-progress-delay-secs", 0);
        appTrackerDelay = getOption(config,"app-tracker-delay-secs", 0);
    }

    public TomlArray getChangeStream(String op){
        return _config.getArrayOrEmpty("changestream." + op);
    }
}
