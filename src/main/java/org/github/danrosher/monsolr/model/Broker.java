package org.github.danrosher.monsolr.model;

import lombok.Data;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

@Data
public class Broker<T> {

    private final ArrayBlockingQueue<T> queue;
    private boolean isRunning = true;

    private int capacity;

    public Broker(int capacity) {
        this.capacity = capacity;
        queue = new ArrayBlockingQueue<>(capacity,true);
    }

    public T take() throws InterruptedException {
        return queue.take();
    }

    public void put(T t) throws InterruptedException {
        put(t, 1, TimeUnit.MILLISECONDS);
    }

    public void put(T t, long timeout, TimeUnit unit) throws InterruptedException {
        while(isRunning) {
            if (queue.offer(t, timeout, unit))
                return;
        }
    }
}
