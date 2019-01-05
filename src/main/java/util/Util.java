package util;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
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
          : Longs.toByteArray(1);
    } catch (IOException e) {
      return Longs.toByteArray(1);
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

  public static void rmrf(String path) throws IOException {
    Files.walk(Paths.get(path))
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }
}
