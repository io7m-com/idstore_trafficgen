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


package com.io7m.idstore.trafficgen.internal;

import com.io7m.idstore.model.IdName;
import com.io7m.idstore.trafficgen.IdTrafficGeneratorConfiguration;
import com.io7m.idstore.trafficgen.IdTrafficGenerators;
import com.io7m.quarrel.core.QCommandContextType;
import com.io7m.quarrel.core.QCommandMetadata;
import com.io7m.quarrel.core.QCommandStatus;
import com.io7m.quarrel.core.QCommandType;
import com.io7m.quarrel.core.QParameterNamed1;
import com.io7m.quarrel.core.QParameterNamedType;
import com.io7m.quarrel.core.QStringType;
import com.io7m.quarrel.ext.logback.QLogback;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * The "run" command.
 */

public final class IdCmdRun implements QCommandType
{
  private static final QParameterNamed1<String> ADMIN_NAME =
    new QParameterNamed1<>(
      "--admin-name",
      List.of(),
      new QStringType.QConstant("The administrator used to create and coordinate users."),
      Optional.empty(),
      String.class
    );

  private static final QParameterNamed1<String> ADMIN_PASSWORD =
    new QParameterNamed1<>(
      "--admin-password",
      List.of(),
      new QStringType.QConstant("The administrator password."),
      Optional.empty(),
      String.class
    );

  private static final QParameterNamed1<URI> ADMIN_API =
    new QParameterNamed1<>(
      "--admin-api",
      List.of(),
      new QStringType.QConstant("The admin API URI."),
      Optional.empty(),
      URI.class
    );

  private static final QParameterNamed1<String> USER_EMAIL_SUFFIX =
    new QParameterNamed1<>(
      "--user-email-suffix",
      List.of(),
      new QStringType.QConstant("The suffix used for created user emails."),
      Optional.empty(),
      String.class
    );

  private static final QParameterNamed1<Duration> USER_PAUSE_DURATION =
    new QParameterNamed1<>(
      "--user-pause-duration",
      List.of(),
      new QStringType.QConstant("The pause between user operations."),
      Optional.empty(),
      Duration.class
    );

  private static final QParameterNamed1<Duration> USER_PAUSE_JITTER =
    new QParameterNamed1<>(
      "--user-pause-jitter",
      List.of(),
      new QStringType.QConstant("Randomize user pauses by at least this duration."),
      Optional.of(Duration.ZERO),
      Duration.class
    );

  private static final QParameterNamed1<Path> USER_PASSWORD_FILE =
    new QParameterNamed1<>(
      "--user-map",
      List.of(),
      new QStringType.QConstant("The user/password map file."),
      Optional.empty(),
      Path.class
    );

  private static final QParameterNamed1<URI> USER_API =
    new QParameterNamed1<>(
      "--user-api",
      List.of(),
      new QStringType.QConstant("The user API URI."),
      Optional.empty(),
      URI.class
    );

  private final QCommandMetadata metadata;

  /**
   * The "traffic-generator" command.
   */

  public IdCmdRun()
  {
    this.metadata = new QCommandMetadata(
      "run",
      new QStringType.QConstant("Run the traffic generator."),
      Optional.empty()
    );
  }

  @Override
  public List<QParameterNamedType<?>> onListNamedParameters()
  {
    return Stream.concat(
      Stream.of(
        ADMIN_API,
        ADMIN_NAME,
        ADMIN_PASSWORD,
        USER_PASSWORD_FILE,
        USER_API,
        USER_EMAIL_SUFFIX,
        USER_PAUSE_DURATION,
        USER_PAUSE_JITTER
      ),
      QLogback.parameters().stream()
    ).toList();
  }

  @Override
  public QCommandStatus onExecute(
    final QCommandContextType context)
    throws Exception
  {
    QLogback.configure(context);

    final var properties = new Properties();
    try (var stream = Files.newInputStream(
      context.parameterValue(USER_PASSWORD_FILE))) {
      properties.load(stream);
    }

    final var map = new HashMap<IdName, String>();
    for (final var name : properties.keySet()) {
      final var nameS = (String) name;
      final var password = properties.getProperty(nameS);
      map.put(new IdName(nameS), password);
    }

    final var configuration =
      new IdTrafficGeneratorConfiguration(
        new IdName(context.parameterValue(ADMIN_NAME)),
        context.parameterValue(ADMIN_PASSWORD),
        context.parameterValue(ADMIN_API),
        context.parameterValue(USER_API),
        context.parameterValue(USER_EMAIL_SUFFIX),
        context.parameterValue(USER_PAUSE_DURATION),
        context.parameterValue(USER_PAUSE_JITTER),
        Map.copyOf(map)
      );

    final var generators = new IdTrafficGenerators();
    try (var generator = generators.create(configuration)) {
      generator.start();

      while (true) {
        Thread.sleep(1_000L);
      }
    }
  }

  @Override
  public QCommandMetadata metadata()
  {
    return this.metadata;
  }
}
