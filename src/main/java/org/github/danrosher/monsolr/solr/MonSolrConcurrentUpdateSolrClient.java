package org.github.danrosher.monsolr.solr;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.http.HttpResponse;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.github.danrosher.monsolr.util.AppConfig;

@Log4j
public class MonSolrConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {

    public MonSolrConcurrentUpdateSolrClient(AppConfig config) {
        this(new ConcurrentUpdateSolrClient.Builder(config.getSolrURL())
            .withQueueSize(config.getNumWriters())
            .withThreadCount(config.getNumWriters())
        );
    }

    protected MonSolrConcurrentUpdateSolrClient(Builder builder) {
        super(builder);
    }

    @Override
    public void onSuccess(HttpResponse resp) {
        super.onSuccess(resp);
    }

    @SneakyThrows
    @Override
    public void handleError(Throwable ex) {
        super.handleError(ex);
        log.error("error:" + ex.getLocalizedMessage());
        throw ex;
    }

}
