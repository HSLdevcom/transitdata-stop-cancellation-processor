package fi.hsl.transitdata.stopcancellation;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

public class ThrottledConsumer {
    private final ExecutorService executorService;

    public ThrottledConsumer(ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Consumes collection using the executor specified in constructor
     * @param items Collection of items
     * @param consumer Consumer
     * @param time Total amount of time (in milliseconds) to be used for consuming the items
     * @param <T>
     */
    public <T> void consumeThrottled(Collection<T> items, Consumer<T> consumer, long time) {
        if (items.isEmpty()) {
            //Nothing to consume
            return;
        }

        executorService.execute(() -> {
            final long delay = time / items.size();
            try {
                for (T item : items) {
                    Thread.sleep(delay);
                    consumer.accept(item);
                }
            } catch (InterruptedException e) {
                //Thread was interrupted
            }
        });
    }
}
