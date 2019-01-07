package util;

import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

public class Util {
  public static long readFile(Path filePath) {
    try {
      return Files.isReadable(filePath)
          ? Longs.fromByteArray(Files.readAllBytes(filePath))
          : 0L;
    } catch (IOException | IllegalArgumentException e) {
      return 0L;
    }
  }

  public static void writeFile(Path filePath, long content) throws Exception {
    Files.createDirectories(filePath.getParent());
    Files.write(
        filePath,
        Longs.toByteArray(content),
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
