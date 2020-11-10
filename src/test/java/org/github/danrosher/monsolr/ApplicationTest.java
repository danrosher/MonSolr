package org.github.danrosher.monsolr;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ApplicationTest {

    @BeforeAll
    public static void beforeClass() {
        BasicConfigurator.resetConfiguration();
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        System.setProperty("config","src/test/resources/test.toml" );
    }

    @Test
    public void simpleTest() throws Throwable {
        Application.main(new String[]{});
    }


}