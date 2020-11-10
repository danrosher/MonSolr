package org.github.danrosher.monsolr.solr;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j;
import org.apache.http.HttpResponse;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.tomlj.TomlParseResult;

import java.util.Optional;

import static org.github.danrosher.monsolr.util.Util.getOption;

@Log4j
public class MonSolrConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {

    public MonSolrConcurrentUpdateSolrClient(TomlParseResult config) {
        this(new ConcurrentUpdateSolrClient.Builder(
                Optional.ofNullable(config.getString("solr-url"))
                    .orElseThrow(() -> new IllegalArgumentException("Must set Solr Url"))
            )
            .withQueueSize(getOption(config,"solr-num-writers",1))
            .withThreadCount(getOption(config,"solr-num-writers",1))
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
        log.error("error:"+ex.getLocalizedMessage());
        throw ex;
    }

}
