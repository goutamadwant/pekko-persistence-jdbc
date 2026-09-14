/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * license agreements; and to You under the Apache License, version 2.0:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * This file is part of the Apache Pekko project, which was derived from Akka.
 */

/*
 * Copyright (C) 2014 - 2019 Dennis Vriend <https://github.com/dnvriend>
 * Copyright (C) 2019 - 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.pekko.persistence.jdbc.snapshot.dao

import org.apache.pekko.persistence.{ SnapshotMetadata, SnapshotSelectionCriteria }

import scala.concurrent.{ ExecutionContext, Future }

trait SnapshotDao {

  /**
   * Load the snapshot with the highest sequence number matching all inclusive criteria bounds.
   * The default delegates to the upper-bound methods when both minimum bounds are zero. For
   * nonzero minimum bounds it loads the highest snapshot within the upper bounds and returns it
   * only if it also satisfies the minimum bounds: any older snapshot has a lower sequence number
   * and, as timestamps do not decrease with the sequence number, a lower timestamp as well.
   * Custom DAOs may override this method to apply all four bounds in a single query.
   */
  def snapshotForCriteria(
      persistenceId: String,
      criteria: SnapshotSelectionCriteria): Future[Option[(SnapshotMetadata, Any)]] =
    criteria match {
      case SnapshotSelectionCriteria(Long.MaxValue, Long.MaxValue, 0L, 0L) =>
        latestSnapshot(persistenceId)
      case SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp, 0L, 0L) =>
        snapshotForMaxTimestamp(persistenceId, maxTimestamp)
      case SnapshotSelectionCriteria(maxSequenceNr, Long.MaxValue, 0L, 0L) =>
        snapshotForMaxSequenceNr(persistenceId, maxSequenceNr)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, 0L, 0L) =>
        snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId, maxSequenceNr, maxTimestamp)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp) =>
        snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId, maxSequenceNr, maxTimestamp)
          .map(_.filter { case (metadata, _) =>
            metadata.sequenceNr >= minSequenceNr && metadata.timestamp >= minTimestamp
          })(ExecutionContext.parasitic)
    }

  /**
   * Delete only snapshots matching all inclusive criteria bounds for this persistence ID.
   * The default delegates to the upper-bound methods when both minimum bounds are zero. For
   * nonzero minimum bounds it never widens the range: it deletes matching snapshots one at a
   * time, loading the highest snapshot within the upper bounds via
   * `snapshotForMaxSequenceNrAndMaxTimestamp`, deleting it with `delete` if it satisfies the
   * minimum bounds, and continuing from the sequence number below it until a snapshot falls
   * outside the interval. Custom DAOs may override this method to apply all four bounds in a
   * single statement.
   */
  def deleteByCriteria(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] =
    criteria match {
      case SnapshotSelectionCriteria(Long.MaxValue, Long.MaxValue, 0L, 0L) =>
        deleteAllSnapshots(persistenceId)
      case SnapshotSelectionCriteria(Long.MaxValue, maxTimestamp, 0L, 0L) =>
        deleteUpToMaxTimestamp(persistenceId, maxTimestamp)
      case SnapshotSelectionCriteria(maxSequenceNr, Long.MaxValue, 0L, 0L) =>
        deleteUpToMaxSequenceNr(persistenceId, maxSequenceNr)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, 0L, 0L) =>
        deleteUpToMaxSequenceNrAndMaxTimestamp(persistenceId, maxSequenceNr, maxTimestamp)
      case SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp, minSequenceNr, minTimestamp) =>
        def deleteDescending(upperSequenceNr: Long): Future[Unit] =
          if (upperSequenceNr < minSequenceNr) Future.unit
          else
            snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId, upperSequenceNr, maxTimestamp).flatMap {
              case Some((metadata, _))
                  if metadata.sequenceNr <= upperSequenceNr && metadata.sequenceNr >= minSequenceNr &&
                  metadata.timestamp >= minTimestamp =>
                delete(persistenceId, metadata.sequenceNr)
                  .flatMap(_ => deleteDescending(metadata.sequenceNr - 1))(ExecutionContext.parasitic)
              case _ => Future.unit
            }(ExecutionContext.parasitic)
        deleteDescending(maxSequenceNr)
    }

  def deleteAllSnapshots(persistenceId: String): Future[Unit]

  def deleteUpToMaxSequenceNr(persistenceId: String, maxSequenceNr: Long): Future[Unit]

  def deleteUpToMaxTimestamp(persistenceId: String, maxTimestamp: Long): Future[Unit]

  def deleteUpToMaxSequenceNrAndMaxTimestamp(
      persistenceId: String,
      maxSequenceNr: Long,
      maxTimestamp: Long): Future[Unit]

  def latestSnapshot(persistenceId: String): Future[Option[(SnapshotMetadata, Any)]]

  def snapshotForMaxTimestamp(persistenceId: String, timestamp: Long): Future[Option[(SnapshotMetadata, Any)]]

  def snapshotForMaxSequenceNr(persistenceId: String, sequenceNr: Long): Future[Option[(SnapshotMetadata, Any)]]

  def snapshotForMaxSequenceNrAndMaxTimestamp(
      persistenceId: String,
      sequenceNr: Long,
      timestamp: Long): Future[Option[(SnapshotMetadata, Any)]]

  def delete(persistenceId: String, sequenceNr: Long): Future[Unit]

  def save(snapshotMetadata: SnapshotMetadata, snapshot: Any): Future[Unit]
}
