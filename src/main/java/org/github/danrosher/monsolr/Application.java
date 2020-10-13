package org.github.danrosher.monsolr;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.github.danrosher.monsolr.exec.NamedPrefixThreadFactory;
import org.github.danrosher.monsolr.export.ChangeStream;
import org.github.danrosher.monsolr.export.DirectRead;
import org.github.danrosher.monsolr.solr.SolrWriters;
import org.tomlj.Toml;
import org.tomlj.TomlParseResult;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

@Log4j
public class Application {

    public static void main(String[] args) throws IOException, InterruptedException, SolrServerException {
        log.info("Start");
        TomlParseResult config = Toml.parse(Paths.get(System.getProperty("config")));
        MongoClient client = new MongoClient(new MongoClientURI(Objects.requireNonNull(config.getString("mongo-url"))));
        ArrayList<Callable<Void>> tasks = new ArrayList<>();
        SolrWriters writers = SolrWriters.builder().config(config).build();
        if (config.contains("mongo-pipeline")) tasks.add(new DirectRead(client, writers, config));
        if (config.contains("changestream")) tasks.add(new ChangeStream(client, writers, config));
        Executors.newCachedThreadPool(new NamedPrefixThreadFactory("app"))
            .invokeAll(tasks);
        log.info("Finish");
    }
}
