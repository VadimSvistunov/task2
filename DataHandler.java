package org.example;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DataHandler implements Handler {
    private final Client client;
    private final ExecutorService executorService;
    private volatile boolean shouldStop = false;
    public DataHandler(Client client) {
        this.client = client;
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public Duration timeout() {
        return Duration.ofMillis(1000);
    }

    @Override
    public void perfomOperation() {
        CompletableFuture<Void> processingFuture = CompletableFuture.runAsync(() -> {
            while (!shouldStop) {
                Event event = client.readData();
                if (event != null) {
                    processEvent(event);
                }
            }
        }, executorService);

        try {
            processingFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void processEvent(Event event) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (Address recipient : event.recipients()) {
            Payload payload = event.payload();
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> sendDataAsync(recipient, payload), executorService);
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private Void sendDataAsync(Address recipient, Payload payload) {
        Result result = Result.REJECTED;
        int attempts = 0;
        // условие с попытками не обязательно, но странно было бы оставлять бесконечный цикл
        // в принципе можно убрать
        while (result == Result.REJECTED && attempts < 10) {
            result = client.sendData(recipient, payload);
            if (result == Result.REJECTED) {
                try {
                    Thread.sleep(timeout().toMillis());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println(recipient.datacenter());
            }
            attempts++;
        }
        return null;
    }

    public void stopOperation() {
        shouldStop = true; // Метод для остановки выполнения операции
    }
}