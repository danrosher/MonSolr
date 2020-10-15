package org.github.danrosher.monsolr.solr;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.tomlj.TomlParseResult;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@Log4j
@RequiredArgsConstructor
@Builder(buildMethodName = "buildInternal")
public class SolrWriters {

    private final TomlParseResult config;
    private BlockingQueue<SolrWriter> $writers;

    private void init() throws IOException, SolrServerException {
        int num_writers = Optional.ofNullable(config.getLong("solr-num-writers")).orElse(1L).intValue();
        String solr_url = Optional.ofNullable(config.getString("solr-url")).orElseThrow(() -> new IllegalArgumentException("Must set Solr Url"));
        $writers = new ArrayBlockingQueue<>(num_writers);
        for (int i = 0; i < num_writers; i++) {
            $writers.add(SolrWriter.builder()
                .solr_url(solr_url)
                .build());
        }
    }

    public void request(UpdateRequest updateRequest, String collection) throws IOException, SolrServerException, InterruptedException {
        SolrWriter writer = $writers.take();
        try {
            log.info("request docs:"+updateRequest.getDocumentsMap());
            //writer.request(updateRequest, collection);
        } finally {
            $writers.put(writer);
        }
    }


    public static class SolrWritersBuilder {
        public SolrWriters build() throws IOException, SolrServerException {
            SolrWriters w = this.buildInternal();
            w.init();
            return w;
        }
    }


}
