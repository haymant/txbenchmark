package com.ongres.benchmark;

import com.google.common.base.Preconditions;
import com.mongodb.MongoCommandException;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.UpdateOptions;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.jooq.lambda.Unchecked;

public class MongoFlightBenchmark implements Runnable, AutoCloseable {

  private final Logger logger = LogManager.getLogger();

  private final AtomicLong idGenerator = new AtomicLong(0);
  private final MongoClient client;
  private final MongoCollection<Document> schedule;
  private final MongoCollection<Document> seat;
  private final MongoCollection<Document> payment;
  private final MongoCollection<Document> audit;
  private final int bookingSleep;
  private final int dayRange;

  private MongoFlightBenchmark(MongoClient client, MongoCollection<Document> schedule,
      MongoCollection<Document> seat, MongoCollection<Document> payment,
      MongoCollection<Document> audit, int bookingSleep, int dayRange) {
    super();
    this.client = client;
    this.schedule = schedule;
    this.seat = seat;
    this.payment = payment;
    this.audit = audit;
    this.bookingSleep = bookingSleep;
    this.dayRange = dayRange;
  }

  /**
   * Create an instance of {@class MongoFlightBenchmark}.
   */
  public static MongoFlightBenchmark create(MongoClient client, String databaseName,
      int bookingSleep, int dayRange) {
    Preconditions.checkArgument(dayRange > 0);
    MongoDatabase database = client.getDatabase(databaseName);
    return new MongoFlightBenchmark(client,
        database.getCollection("schedule"),
        database.getCollection("seat"),
        database.getCollection("payment"),
        database.getCollection("audit"),
        bookingSleep, dayRange);
  }

  @Override
  public void run() {
    Unchecked.runnable(this::userOperation).run();
  }

  private Object generateUserId() {
    Object userId = idGenerator.getAndIncrement();
    return userId;
  }

  private void userOperation() throws Exception {
    try (ClientSession session = client.startSession()) {
      session.startTransaction(TransactionOptions.builder()
          .writeConcern(WriteConcern.MAJORITY)
          .build());
      try {
        final Document userSchedule = getUserSchedule(session);
        final Object userId = generateUserId();
        final Instant now = Instant.now();
        final Timestamp currentTimestamp = Timestamp.from(now);
        final Date day = Date.valueOf(LocalDate.now().plus(
            now.toEpochMilli() % dayRange, ChronoUnit.DAYS));
        TimeUnit.SECONDS.sleep(bookingSleep);
        insertSeat(session, userSchedule, userId, currentTimestamp);
        insertPayment(session, userSchedule, userId, currentTimestamp);
        insertAudit(session, userSchedule, day, currentTimestamp);
        session.commitTransaction();
      } catch (Exception ex) {
        if (ex instanceof MongoCommandException
            && ((MongoCommandException) ex).getErrorCode() == 112) {
          throw new RetryUserOperationException(ex);
        }
        try {
          session.abortTransaction();
        } catch (Exception abortEx) {
          logger.error(abortEx);
        }
        throw ex;
      }
    }
  }

  private Document getUserSchedule(ClientSession clientSession) {
    AggregateIterable<Document> schedules = schedule.aggregate(clientSession,
        Arrays.asList(
            Aggregates.sample(1),
            Aggregates.lookup("aircraft", "aircraft", "iata", "aircraft"),
            Aggregates.project(new Document()
                .append("_id", 1)
                .append("duration", 1)
                .append("capacity", "$aircraft.capacity"))));
    return schedules.first();
  }

  private void insertSeat(ClientSession session, Document userSchedule,
      Object userId, Timestamp currentTimestamp) {
    seat.insertOne(session, new Document()
        .append("user_id", userId)
        .append("schedule_id", userSchedule.getObjectId("_id"))
        .append("date", currentTimestamp));
  }

  private void insertPayment(ClientSession session, Document userSchedule,
      Object userId, Timestamp currentTimestamp) {
    payment.insertOne(session, new Document()
        .append("user_id", userId)
        .append("amount", Optional.ofNullable(userSchedule.getString("duration"))
            .map(d -> d.split(":"))
            .map(s -> Integer.parseInt(s[0]) * 60 + Integer.parseInt(s[1]))
            .map(d -> Math.max(42, d * 42))
            .orElse(42))
        .append("date", currentTimestamp));
  }

  private void insertAudit(ClientSession session, Document userSchedule,
      Date day, Timestamp currentTimestamp) {
    audit.updateOne(session, 
        new Document()
        .append("schedule_id", userSchedule.getObjectId("_id"))
        .append("day", day), 
        new Document()
        .append("$set", new Document().append("date", currentTimestamp))
        .append("$inc", new Document().append("seats_occupied", 1)),
        new UpdateOptions().upsert(true));
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}