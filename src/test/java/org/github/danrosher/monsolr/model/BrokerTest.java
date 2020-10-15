package org.github.danrosher.monsolr.model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {

    Broker<Integer> broker;

    @BeforeEach
    void setUp() {
        broker = new Broker<>(3);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void take() throws InterruptedException {
        broker.put(1);
        assertEquals(1, broker.take());
    }


    @Test
    @DisplayName("thisIsATst")
    void put() throws InterruptedException {
        broker.put(1);
        broker.put(2);
        assertEquals(1, broker.take());
        assertEquals(2, broker.take());
    }

    @Test
    void testPut() {
    }
}