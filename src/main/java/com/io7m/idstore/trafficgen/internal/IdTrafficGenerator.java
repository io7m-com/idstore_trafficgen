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

import com.io7m.idstore.admin_client.IdAClients;
import com.io7m.idstore.admin_client.api.IdAClientConfiguration;
import com.io7m.idstore.admin_client.api.IdAClientCredentials;
import com.io7m.idstore.admin_client.api.IdAClientException;
import com.io7m.idstore.model.IdEmail;
import com.io7m.idstore.model.IdName;
import com.io7m.idstore.model.IdPasswordAlgorithmPBKDF2HmacSHA256;
import com.io7m.idstore.model.IdPasswordException;
import com.io7m.idstore.model.IdRealName;
import com.io7m.idstore.protocol.admin.IdACommandUserCreate;
import com.io7m.idstore.protocol.user.IdUCommandPasswordUpdate;
import com.io7m.idstore.trafficgen.IdTrafficGeneratorConfiguration;
import com.io7m.idstore.trafficgen.IdTrafficGeneratorType;
import com.io7m.idstore.user_client.IdUClients;
import com.io7m.idstore.user_client.api.IdUClientConfiguration;
import com.io7m.idstore.user_client.api.IdUClientCredentials;
import com.io7m.idstore.user_client.api.IdUClientException;
import com.io7m.idstore.user_client.api.IdUClientSynchronousType;
import io.opentelemetry.api.OpenTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.io7m.idstore.error_codes.IdStandardErrorCodes.USER_DUPLICATE_ID_NAME;

/**
 * The default traffic generator.
 */

public final class IdTrafficGenerator implements IdTrafficGeneratorType
{
  private static final Logger LOG =
    LoggerFactory.getLogger(IdTrafficGenerator.class);

  private final ExecutorService executor;
  private final IdTrafficGeneratorConfiguration configuration;
  private final SecureRandom rng;
  private final AtomicBoolean running;

  /**
   * The default traffic generator.
   *
   * @param inExecutor      The executor
   * @param inConfiguration The configuration
   * @param inRNG           A random number generator
   */

  public IdTrafficGenerator(
    final ExecutorService inExecutor,
    final IdTrafficGeneratorConfiguration inConfiguration,
    final SecureRandom inRNG)
  {
    this.executor =
      Objects.requireNonNull(inExecutor, "executor");
    this.configuration =
      Objects.requireNonNull(inConfiguration, "configuration");
    this.rng =
      Objects.requireNonNull(inRNG, "rng");
    this.running =
      new AtomicBoolean(false);
  }

  @Override
  public void start()
  {
    if (this.running.compareAndSet(false, true)) {
      this.executor.execute(this::runMainCoordinator);
    }
  }

  private void runMainCoordinator()
  {
    try {
      this.runSetupUsers();
      this.runMainLoop();
    } catch (final Throwable ex) {
      LOG.error("coordinator: ", ex);
    } finally {
      this.stop();
    }
  }

  private void runSetupUsers()
    throws IdAClientException, InterruptedException
  {
    final var adminClients =
      new IdAClients();
    final var userClients =
      new IdUClients();
    final var adminConfig =
      new IdAClientConfiguration(Locale.ROOT);
    final var userConfig =
      new IdUClientConfiguration(OpenTelemetry.noop(), Locale.ROOT);

    try (var adminClient = adminClients.openSynchronousClient(adminConfig)) {
      final var adminName = this.configuration.adminName().value();
      LOG.info("logging in as administrator {}", adminName);

      adminClient.loginOrElseThrow(new IdAClientCredentials(
        adminName,
        this.configuration.password(),
        this.configuration.adminAPI(),
        Map.of()
      ), IdAClientException::ofError);

      for (final var entry : this.configuration.users().entrySet()) {
        final var userName =
          entry.getKey();
        final var password =
          entry.getValue();

        try {
          adminClient.executeOrElseThrow(
            new IdACommandUserCreate(
              Optional.empty(),
              userName,
              new IdRealName("Traffic Generator"),
              new IdEmail(userName.value() + this.configuration.userEmailSuffix()),
              IdPasswordAlgorithmPBKDF2HmacSHA256.create()
                .createHashed(password)
            ),
            IdAClientException::ofError
          );
        } catch (final IdPasswordException e) {
          LOG.error("password generation: ", e);
        } catch (final IdAClientException e) {
          if (e.errorCode().equals(USER_DUPLICATE_ID_NAME)) {
            LOG.info("user {} already exists", userName);
          } else {
            throw e;
          }
        }

        this.executor.execute(() -> {
          this.runUserTask(
            userName,
            password,
            userClients.openSynchronousClient(userConfig)
          );
        });
      }
    }
  }

  private void runUserTask(
    final IdName userName,
    final String password,
    final IdUClientSynchronousType client)
  {
    LOG.info("[{}] started user task", userName);

    try {
      while (this.running.get()) {
        try {
          client.loginOrElseThrow(new IdUClientCredentials(
            userName.value(),
            password,
            this.configuration.userAPI(),
            Map.of()
          ), IdUClientException::ofError);
          LOG.info("[{}] logged in", userName);
          break;
        } catch (final IdUClientException e) {
          LOG.error("[{}] login: ", userName, e);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        this.userPause();
      }

      while (this.running.get()) {
        try {
          LOG.info("[{}] updating password", userName);
          client.executeOrElseThrow(
            new IdUCommandPasswordUpdate(password, password),
            IdUClientException::ofError
          );
        } catch (final IdUClientException e) {
          LOG.error("[{}] password update: ", userName, e);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        this.userPause();
      }
    } finally {
      try {
        client.close();
      } catch (final IdUClientException e) {
        LOG.error("[{}] close: ", userName, e);
      }
    }
  }

  private void userPause()
  {
    try {
      Thread.sleep(this.configuration.pauseBetweenTasks().toMillis());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    try {
      final var bound =
        this.configuration.pauseJitter().toMillis();
      final var time =
        this.rng.nextDouble() * (double) bound;
      Thread.sleep((long) time);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void runMainLoop()
  {
    while (true) {
      try {
        if (!this.running.get()) {
          return;
        }

      } catch (final Throwable ex) {
        LOG.error("coordinator: ", ex);
      }
    }
  }

  @Override
  public void stop()
  {
    this.running.set(false);
  }

  @Override
  public void close()
  {
    this.stop();
    this.executor.shutdown();
  }

  @Override
  public String toString()
  {
    return String.format(
      "[IdTrafficGenerator 0x%s]",
      Long.toUnsignedString(System.identityHashCode(this), 16)
    );
  }
}
