package fi.hsl.transitdata.stopcancellation;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ThrottledConsumerTest {
    @Test
    public void testThrottledConsumer() throws InterruptedException {
        ThrottledConsumer throttledConsumer = new ThrottledConsumer(Executors.newSingleThreadExecutor());

        List<String> values = Arrays.asList("a", "b", "c");
        CountDownLatch countDownLatch = new CountDownLatch(values.size());

        List<String> output = new ArrayList<>();

        long start = System.currentTimeMillis();

        throttledConsumer.consumeThrottled(values, value -> {
            output.add(value);
            countDownLatch.countDown();
        }, 5000);

        countDownLatch.await();

        long duration = System.currentTimeMillis() - start;

        assertTrue(duration >= 5000);
        assertArrayEquals(new String[] { "a", "b", "c" }, output.toArray());
    }

    @Test
    public void testThrottledConsumerWhenInterrupted() throws InterruptedException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        ThrottledConsumer throttledConsumer = new ThrottledConsumer(executorService);

        List<String> values = Arrays.asList("a", "b", "c");
        List<String> output = new ArrayList<>();

        throttledConsumer.consumeThrottled(values, output::add, Long.MAX_VALUE);

        Thread.sleep(1000);

        executorService.shutdownNow();
        executorService.awaitTermination(1, TimeUnit.SECONDS);

        assertEquals(0, output.size());
    }
}
