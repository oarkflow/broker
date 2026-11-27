import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class KafkaParallelProducer {
  public static void main(String[] args) throws Exception {
    String topic = args[0];
    long totalRecords = Long.parseLong(args[1]);
    int recordSize = Integer.parseInt(args[2]);
    int partitions = Integer.parseInt(args[3]);
    int concurrency = Integer.parseInt(args[4]);
    int batchSize = Integer.parseInt(args[5]);
    byte[] payload = new byte[recordSize];

    CountDownLatch ready = new CountDownLatch(concurrency);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(concurrency);
    AtomicLong sent = new AtomicLong();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Long> batchLatencyNanos = Collections.synchronizedList(new ArrayList<>());
    long[] producerRecords = new long[concurrency];
    for (int i = 0; i < concurrency; i++) {
      producerRecords[i] = totalRecords / concurrency + (i < totalRecords % concurrency ? 1 : 0);
    }

    for (int id = 0; id < concurrency; id++) {
      final int producerID = id;
      final long records = producerRecords[id];
      Thread thread = new Thread(() -> {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-bench:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(1 << 20));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "broker-bench-" + producerID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
          ready.countDown();
          start.await();
          long offset = 0;
          while (offset < records) {
            int n = (int) Math.min(batchSize, records - offset);
            List<Future<RecordMetadata>> futures = new ArrayList<>(n);
            long began = System.nanoTime();
            int partition = producerID % partitions;
            for (int j = 0; j < n; j++) {
              futures.add(producer.send(new ProducerRecord<>(topic, partition, null, payload)));
            }
            for (Future<RecordMetadata> future : futures) future.get();
            batchLatencyNanos.add(System.nanoTime() - began);
            offset += n;
            sent.addAndGet(n);
          }
        } catch (Throwable t) {
          failure.compareAndSet(null, t);
        } finally {
          ready.countDown();
          done.countDown();
        }
      }, "kafka-bench-producer-" + id);
      thread.start();
    }

    ready.await();
    long began = System.nanoTime();
    start.countDown();
    done.await();
    long elapsed = System.nanoTime() - began;
    Throwable error = failure.get();
    if (error != null) throw new RuntimeException("Kafka producer failed", error);
    if (sent.get() != totalRecords) throw new IllegalStateException("sent " + sent + " of " + totalRecords);

    Collections.sort(batchLatencyNanos);
    long p50 = percentile(batchLatencyNanos, .50);
    long p95 = percentile(batchLatencyNanos, .95);
    long p99 = percentile(batchLatencyNanos, .99);
    double seconds = elapsed / 1_000_000_000.0;
    System.out.printf("mode=publish records=%d errors=0 elapsed_seconds=%.6f msg/s=%.1f producers=%d batch=%d batch_p50_ms=%.3f batch_p95_ms=%.3f batch_p99_ms=%.3f%n",
        sent.get(), seconds, sent.get() / seconds, concurrency, batchSize,
        p50 / 1_000_000.0, p95 / 1_000_000.0, p99 / 1_000_000.0);
  }

  private static long percentile(List<Long> values, double q) {
    if (values.isEmpty()) return 0;
    return values.get((int) ((values.size() - 1) * q));
  }
}
