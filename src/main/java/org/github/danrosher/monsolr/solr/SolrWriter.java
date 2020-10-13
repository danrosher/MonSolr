package org.github.danrosher.monsolr.solr;

import lombok.Builder;
import lombok.extern.log4j.Log4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;

@Builder(buildMethodName = "buildInternal")
@Log4j
public class SolrWriter {

    private final String solr_url;
    private SolrClient client;

    private final int retryAttempts;
    private final int waitSecsBetweenRetries;
    private final int connectionTimeoutMillis;
    private final int socketTimeoutMillis;


    private void init() {
        client = new ConcurrentUpdateSolrClient.Builder(solr_url).withConnectionTimeout(connectionTimeoutMillis)
            .withSocketTimeout(socketTimeoutMillis).build();
    }

    public NamedList<Object> request(UpdateRequest updateRequest,String collection) throws IOException, SolrServerException {
        return request(updateRequest, collection,retryAttempts);
    }

    public NamedList<Object> request(UpdateRequest updateRequest, String collection, int remainingRetryAttempts) throws IOException, SolrServerException {
        try {
            return client.request(updateRequest, collection);
            //remote issue?
        } catch (BaseHttpSolrClient.RemoteSolrException ex){
            throw ex;
            //...or local?
        } catch (Exception e) {
            if (remainingRetryAttempts <= 0) {
                log.error("Send update request to " + collection + " failed due to " + e
                    + "; max number of re-try attempts " + retryAttempts
                    + " reached, no more attempts available, request fails!");
                throw e;
            }
            log.error("Send update request to " + collection + " failed due to " + e
                + "; will retry after waiting " + waitSecsBetweenRetries + " secs");
            try {
                Thread.sleep(waitSecsBetweenRetries * 1000L);
            } catch (InterruptedException ie) {
                log.error("Sleeping thread interrupted");
                Thread.currentThread().interrupt();
            }
            return request(updateRequest, collection, --remainingRetryAttempts);
        }
    }

    public static class SolrWriterBuilder {
        public SolrWriter build() throws IOException, SolrServerException {
            SolrWriter w = this.buildInternal();
            w.init();
            return w;
        }
    }



}
