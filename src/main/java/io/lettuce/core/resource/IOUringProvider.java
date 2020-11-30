package io.lettuce.core.resource;

import io.lettuce.core.internal.LettuceAssert;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.internal.SystemPropertyUtil;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

public class IOUringProvider {

  private static final InternalLogger logger = InternalLoggerFactory
      .getInstance(IOUringProvider.class);

  private static final String IO_URING_ENABLED_KEY = "io.lettuce.core.io_uring";

  private static final boolean IO_URING_ENABLED = Boolean.parseBoolean(
      SystemPropertyUtil.get(IO_URING_ENABLED_KEY, "true"));

  private static final boolean IO_URING_AVAILABLE;

  private static final EventLoopResources IO_URING_RESOURCES;

  static {

    boolean availability;
    try {
      Class.forName("io.netty.incubator.channel.uring.IOUring");
      availability = IOUring.isAvailable();
    } catch (ClassNotFoundException e) {
      availability = false;
    }

    IO_URING_AVAILABLE = availability;

    if (IO_URING_AVAILABLE) {
      logger.debug("Starting with io_uring library");
      IO_URING_RESOURCES = IOUringProvider.AvailableIOUringResources.INSTANCE;

    } else {
      logger.debug("Starting without optional io_uring library");
      IO_URING_RESOURCES = IOUringProvider.UnavailableIOUringResources.INSTANCE;
    }
  }

  /**
   * @return {@code true} if io_uring is available.
   */
  public static boolean isAvailable() {
    return IO_URING_AVAILABLE && IO_URING_ENABLED;
  }

  /**
   * Check whether the io_uring library is available on the class path.
   *
   * @throws IllegalStateException if the {@literal netty-incubator-transport-native-io_uring}
   * library is not available
   */
  static void checkForIOUringLibrary() {

    LettuceAssert.assertState(IO_URING_ENABLED,
        String.format("io_uring use is disabled via System properties (%s)", IO_URING_ENABLED_KEY));
    LettuceAssert.assertState(isAvailable(),
        "netty-incubator-transport-native-io_uring is not available. Make sure netty-incubator-transport-native-io_uring library on the class path and supported by your operating system.");
  }

  /**
   * Returns the {@link EventLoopResources} for io_uring-backed transport. Check availability with
   * {@link #isAvailable()} prior to obtaining the resources.
   *
   * @return the {@link EventLoopResources}. May be unavailable.
   * @since xxx
   */
  public static EventLoopResources getResources() {
    return IO_URING_RESOURCES;
  }

  /**
   * {@link EventLoopResources} for unavailable IOUring.
   */
  enum UnavailableIOUringResources implements EventLoopResources {

    INSTANCE;

    @Override
    public Class<? extends Channel> domainSocketChannelClass() {

      checkForIOUringLibrary();
      return null;
    }

    @Override
    public Class<? extends EventLoopGroup> eventLoopGroupClass() {

      checkForIOUringLibrary();
      return null;
    }

    @Override
    public boolean matches(Class<? extends EventExecutorGroup> type) {

      checkForIOUringLibrary();
      return false;
    }

    @Override
    public EventLoopGroup newEventLoopGroup(int nThreads, ThreadFactory threadFactory) {

      checkForIOUringLibrary();
      return null;
    }

    @Override
    public SocketAddress newSocketAddress(String socketPath) {

      checkForIOUringLibrary();
      return null;
    }

    @Override
    public Class<? extends Channel> socketChannelClass() {

      checkForIOUringLibrary();
      return null;
    }

  }

  /**
   * {@link EventLoopResources} for available io_uring.
   */
  enum AvailableIOUringResources implements EventLoopResources {

    INSTANCE;

    @Override
    public boolean matches(Class<? extends EventExecutorGroup> type) {

      LettuceAssert.notNull(type, "EventLoopGroup type must not be null");

      return type.equals(eventLoopGroupClass());
    }

    @Override
    public EventLoopGroup newEventLoopGroup(int nThreads, ThreadFactory threadFactory) {

      checkForIOUringLibrary();

      return new IOUringEventLoopGroup(nThreads, threadFactory);
    }

    @Override
    public Class<? extends Channel> domainSocketChannelClass() {

      checkForIOUringLibrary();

      throw new IllegalStateException(
          "io_uring library does not support domain socket yet");
    }

    @Override
    public Class<? extends Channel> socketChannelClass() {

      checkForIOUringLibrary();

      return IOUringSocketChannel.class;
    }

    @Override
    public Class<? extends EventLoopGroup> eventLoopGroupClass() {

      checkForIOUringLibrary();

      return IOUringEventLoopGroup.class;
    }

    @Override
    public SocketAddress newSocketAddress(String socketPath) {

      checkForIOUringLibrary();

      throw new IllegalStateException(
          "io_uring library does not support domain socket yet");
    }

  }

}
