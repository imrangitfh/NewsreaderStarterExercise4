package at.ac.fhcampuswien.newsanalyzer.downloader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ParallelDownloader extends Downloader {
    ExecutorService execService = Executors.newFixedThreadPool(5);
    @Override
    public int process(List<String> urls, List<String> titles) throws InterruptedException {
        int count = 0;
        for (String url : urls) {
            try {
                int finalCount = count;
                Future<?> taskStatus = execService.submit(() -> {
                    System.out.printf("Starting Thread %s\n", Thread.currentThread().getName());
                    System.out.println("Downloaded: " + url);
                    saveUrl2File(url, titles.get(finalCount));
                });
                count++;
            } catch (Exception e) {
                System.out.println("Problem with Thread: " + Thread.currentThread().getName());
            }
        }
        execService.shutdown();
        try {
            if (!execService.awaitTermination(60, TimeUnit.SECONDS)) {
                execService.shutdownNow();
            }
        } catch (InterruptedException ex) {
            execService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        return count;
    }
}
