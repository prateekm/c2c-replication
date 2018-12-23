package util;

import com.google.common.primitives.Ints;

import java.util.Random;

public class Constants {
  public static final Random RANDOM = new Random();

  public static final String SERVER_HOST = "127.0.0.1";

  public static final int OPCODE_SYNC_INT = 0;
  public static final byte[] OPCODE_SYNC = Ints.toByteArray(OPCODE_SYNC_INT);
  public static final int OPCODE_WRITE_INT = 1;
  public static final byte[] OPCODE_WRITE = Ints.toByteArray(OPCODE_WRITE_INT);
  public static final int OPCODE_COMMIT_INT = 2;
  public static final byte[] OPCODE_COMMIT = Ints.toByteArray(OPCODE_COMMIT_INT);
}
