package org.github.danrosher.monsolr;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ApplicationTest {

    @BeforeAll
    public static void beforeClass() throws Exception {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        System.setProperty("config","src/test/resources/test.toml" );
    }

    @Test
    public void simpleTest() throws IOException, InterruptedException, SolrServerException {
        Application.main(new String[]{});
    }


}