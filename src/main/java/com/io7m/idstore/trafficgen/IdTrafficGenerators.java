/*
 * Copyright © 2023 Mark Raynsford <code@io7m.com> https://www.io7m.com
 *
 * Permission to use, copy, modify, and/or distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
 * IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */


package com.io7m.idstore.trafficgen;

import com.io7m.idstore.trafficgen.internal.IdTrafficGenerator;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.concurrent.Executors;

/**
 * The default traffic generator factory.
 */

public final class IdTrafficGenerators
  implements IdTrafficGeneratorFactoryType
{
  /**
   * The default traffic generator factory.
   */

  public IdTrafficGenerators()
  {

  }

  @Override
  public IdTrafficGeneratorType create(
    final IdTrafficGeneratorConfiguration configuration)
  {
    Objects.requireNonNull(configuration, "configuration");

    final var executor =
      Executors.newCachedThreadPool(r -> {
        final var thread =
          new Thread(null, r, "com.io7m.idstore.trafficgen", 1048576L);
        thread.setName(
          "com.io7m.idstore.trafficgen[%d]".formatted(thread.getId()));
        thread.setDaemon(true);
        return thread;
      });

    try {
      return new IdTrafficGenerator(
        executor,
        configuration,
        SecureRandom.getInstanceStrong()
      );
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public String toString()
  {
    return String.format(
      "[IdTrafficGenerators 0x%s]",
      Long.toUnsignedString(System.identityHashCode(this), 16)
    );
  }
}
