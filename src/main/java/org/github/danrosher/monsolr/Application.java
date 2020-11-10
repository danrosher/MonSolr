package org.github.danrosher.monsolr;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrClient;
import org.github.danrosher.monsolr.exec.NamedPrefixThreadFactory;
import org.github.danrosher.monsolr.export.ChangeStream;
import org.github.danrosher.monsolr.export.DirectRead;
import org.github.danrosher.monsolr.solr.MonSolrConcurrentUpdateSolrClient;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

@Log4j
public class Application {

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("Start");
        TomlParseResult config = Toml.parse(Paths.get(System.getProperty("config")));
        MongoClient mongoClient = new MongoClient(new MongoClientURI(Objects.requireNonNull(config.getString("mongo-url"))));
        ArrayList<Callable<Void>> tasks = new ArrayList<>();
        SolrClient solrClient = new MonSolrConcurrentUpdateSolrClient(config);
        if (config.contains("mongo-pipeline")) tasks.add(new DirectRead(mongoClient, solrClient, config));
        if (config.contains("changestream")) tasks.add(new ChangeStream(mongoClient, solrClient, config));
        Executors.newCachedThreadPool(new NamedPrefixThreadFactory("app"))
            .invokeAll(tasks);
        log.info("Finish");
    }
}
