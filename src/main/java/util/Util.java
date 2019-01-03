package util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.primitives.Ints;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

public class Util {
  public static void initMetrics(MetricRegistry metrics, Logger logger) {
    final Slf4jReporter logReporter = Slf4jReporter.forRegistry(metrics)
        .outputTo(logger)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.NANOSECONDS)
        .build();
    logReporter.start(5, TimeUnit.SECONDS);

    final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.NANOSECONDS)
        .build();
    jmxReporter.start();
  }

  public static byte[] readFile(Path filePath) {
    try {
      return Files.isReadable(filePath)
          ? Files.readAllBytes(filePath)
          : Ints.toByteArray(1); // TODO rocksdb fails assert if starting messageId is 0
    } catch (IOException e) {
      return Ints.toByteArray(1); // TODO rocksdb fails assert if starting messageId is 0
    }
  }

  public static void writeFile(Path filePath, byte[] content) throws Exception {
    Files.createDirectories(filePath.getParent());
    Files.write(
        filePath,
        content,
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE,
        StandardOpenOption.SYNC);
  }
}
