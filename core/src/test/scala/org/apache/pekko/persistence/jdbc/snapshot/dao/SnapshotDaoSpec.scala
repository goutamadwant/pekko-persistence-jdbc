/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pekko.persistence.jdbc.snapshot.dao

import org.apache.pekko.persistence.{ SnapshotMetadata, SnapshotSelectionCriteria }
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.SortedMap
import scala.concurrent.Future

class SnapshotDaoSpec extends AnyWordSpec with Matchers with ScalaFutures {

  /**
   * Implements only the abstract `SnapshotDao` methods over an in-memory map, the way a custom
   * DAO written before the criteria methods existed would, and records every call made to it.
   */
  private class CustomSnapshotDao(seed: Long*) extends SnapshotDao {
    var calls = Vector.empty[(String, String, Seq[Long])]
    var rows: SortedMap[Long, (SnapshotMetadata, Any)] =
      SortedMap.from(seed.map(nr => nr -> ((SnapshotMetadata("custom", nr, nr * 100), s"snapshot-$nr"))))
    var failDeleteOf: Option[Long] = None

    def sequenceNrs: Seq[Long] = rows.keys.toSeq

    private def record(method: String, persistenceId: String, bounds: Long*): Unit =
      calls :+= ((method, persistenceId, bounds.toList))

    private def highest(maxSequenceNr: Long, maxTimestamp: Long) =
      Future.successful(rows.values.filter { case (metadata, _) =>
        metadata.sequenceNr <= maxSequenceNr && metadata.timestamp <= maxTimestamp
      }.lastOption)

    private def deleteUpTo(maxSequenceNr: Long, maxTimestamp: Long) = {
      rows = rows.filterNot { case (_, (metadata, _)) =>
        metadata.sequenceNr <= maxSequenceNr && metadata.timestamp <= maxTimestamp
      }
      Future.unit
    }

    override def latestSnapshot(persistenceId: String) = {
      record("latest", persistenceId)
      highest(Long.MaxValue, Long.MaxValue)
    }
    override def snapshotForMaxTimestamp(persistenceId: String, timestamp: Long) = {
      record("timestamp", persistenceId, timestamp)
      highest(Long.MaxValue, timestamp)
    }
    override def snapshotForMaxSequenceNr(persistenceId: String, sequenceNr: Long) = {
      record("sequence", persistenceId, sequenceNr)
      highest(sequenceNr, Long.MaxValue)
    }
    override def snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId: String, sequenceNr: Long, timestamp: Long) = {
      record("both", persistenceId, sequenceNr, timestamp)
      highest(sequenceNr, timestamp)
    }

    override def deleteAllSnapshots(persistenceId: String) = {
      record("latest", persistenceId)
      deleteUpTo(Long.MaxValue, Long.MaxValue)
    }
    override def deleteUpToMaxTimestamp(persistenceId: String, timestamp: Long) = {
      record("timestamp", persistenceId, timestamp)
      deleteUpTo(Long.MaxValue, timestamp)
    }
    override def deleteUpToMaxSequenceNr(persistenceId: String, sequenceNr: Long) = {
      record("sequence", persistenceId, sequenceNr)
      deleteUpTo(sequenceNr, Long.MaxValue)
    }
    override def deleteUpToMaxSequenceNrAndMaxTimestamp(persistenceId: String, sequenceNr: Long, timestamp: Long) = {
      record("both", persistenceId, sequenceNr, timestamp)
      deleteUpTo(sequenceNr, timestamp)
    }

    override def delete(persistenceId: String, sequenceNr: Long) = {
      record("single", persistenceId, sequenceNr)
      if (failDeleteOf.contains(sequenceNr)) Future.failed(new IllegalStateException(s"delete $sequenceNr"))
      else {
        rows -= sequenceNr
        Future.unit
      }
    }
    override def save(metadata: SnapshotMetadata, snapshot: Any) =
      Future.failed(new UnsupportedOperationException("save"))
  }

  private def load(persistenceId: String, sequenceNr: Long, timestamp: Long) =
    ("both", persistenceId, List(sequenceNr, timestamp))
  private def single(persistenceId: String, sequenceNr: Long) = ("single", persistenceId, List(sequenceNr))

  "SnapshotDao criteria defaults" should {
    val upperBounds = Seq(
      (SnapshotSelectionCriteria.Latest, "latest", Seq.empty[Long]),
      (SnapshotSelectionCriteria(maxTimestamp = 300), "timestamp", Seq(300L)),
      (SnapshotSelectionCriteria(maxSequenceNr = 3), "sequence", Seq(3L)),
      (SnapshotSelectionCriteria(3, 300), "both", Seq(3L, 300L)))

    upperBounds.foreach { case (criteria, method, arguments) =>
      s"preserve the existing $method load and delete methods" in {
        val dao = new CustomSnapshotDao(1, 2, 3)
        dao.snapshotForCriteria("custom", criteria).futureValue shouldBe dao.rows.get(3)
        dao.calls shouldBe Vector((method, "custom", arguments))
        dao.deleteByCriteria("custom", criteria).futureValue shouldBe (())
        dao.calls shouldBe Vector.fill(2)((method, "custom", arguments))
        dao.sequenceNrs shouldBe empty
      }
    }

    "load the highest snapshot within all four bounds" in {
      val dao = new CustomSnapshotDao(1, 2, 3, 4, 5, 6)
      dao.snapshotForCriteria("custom", SnapshotSelectionCriteria(5, 450, 2, 250)).futureValue shouldBe dao.rows.get(4)
      dao.calls shouldBe Vector(load("custom", 5, 450))
    }

    "load nothing when the highest snapshot within the upper bounds is below the minimum sequence number" in {
      val dao = new CustomSnapshotDao(1, 2, 3)
      dao.snapshotForCriteria("custom", SnapshotSelectionCriteria(minSequenceNr = 4)).futureValue shouldBe None
      dao.calls shouldBe Vector(load("custom", Long.MaxValue, Long.MaxValue))
    }

    "load nothing when the highest snapshot within the upper bounds is below the minimum timestamp" in {
      val dao = new CustomSnapshotDao(1, 2, 3)
      dao.snapshotForCriteria("custom", SnapshotSelectionCriteria(maxSequenceNr = 2, minTimestamp = 250))
        .futureValue shouldBe None
      dao.calls shouldBe Vector(load("custom", 2, Long.MaxValue))
    }

    "load nothing for an empty interval" in {
      val dao = new CustomSnapshotDao(1, 2, 3)
      dao.snapshotForCriteria("custom", SnapshotSelectionCriteria(maxSequenceNr = 1, minSequenceNr = 2))
        .futureValue shouldBe None
    }

    "delete only the snapshots within the sequence number interval" in {
      val dao = new CustomSnapshotDao(1, 2, 3, 4, 5, 6)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(maxSequenceNr = 5, minSequenceNr = 2)).futureValue
      dao.sequenceNrs shouldBe Seq(1L, 6L)
      dao.calls shouldBe Vector(
        load("custom", 5, Long.MaxValue),
        single("custom", 5),
        load("custom", 4, Long.MaxValue),
        single("custom", 4),
        load("custom", 3, Long.MaxValue),
        single("custom", 3),
        load("custom", 2, Long.MaxValue),
        single("custom", 2))
    }

    "delete only the snapshots at or above the minimum timestamp" in {
      val dao = new CustomSnapshotDao(1, 2, 3, 4, 5, 6)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(minTimestamp = 350)).futureValue
      dao.sequenceNrs shouldBe Seq(1L, 2L, 3L)
      dao.calls.last shouldBe load("custom", 3, Long.MaxValue)
    }

    "delete only the snapshots within all four bounds" in {
      val dao = new CustomSnapshotDao(1, 2, 3, 4, 5, 6)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(5, 450, 2, 250)).futureValue
      dao.sequenceNrs shouldBe Seq(1L, 2L, 5L, 6L)
      dao.calls shouldBe Vector(
        load("custom", 5, 450),
        single("custom", 4),
        load("custom", 3, 450),
        single("custom", 3),
        load("custom", 2, 450))
    }

    "delete nothing for an empty interval" in {
      val dao = new CustomSnapshotDao(1, 2, 3)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(maxSequenceNr = 1, minSequenceNr = 2)).futureValue
      dao.sequenceNrs shouldBe Seq(1L, 2L, 3L)
      dao.calls shouldBe empty
    }

    "delete nothing when no snapshot is within the upper bounds" in {
      val dao = new CustomSnapshotDao(4, 5)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(maxSequenceNr = 3, minSequenceNr = 1)).futureValue
      dao.sequenceNrs shouldBe Seq(4L, 5L)
      dao.calls shouldBe Vector(load("custom", 3, Long.MaxValue))
    }

    "treat negative minimum bounds like the built-in DAOs and match every snapshot" in {
      val dao = new CustomSnapshotDao(1, 2, 3)
      dao.snapshotForCriteria("custom", SnapshotSelectionCriteria(minSequenceNr = -1)).futureValue shouldBe
      dao.rows
        .get(3)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(minTimestamp = -1)).futureValue
      dao.sequenceNrs shouldBe empty
    }

    "stop deleting and fail when a single delete fails" in {
      val dao = new CustomSnapshotDao(1, 2, 3, 4)
      dao.failDeleteOf = Some(3)
      dao.deleteByCriteria("custom", SnapshotSelectionCriteria(maxSequenceNr = 4, minSequenceNr = 2)).failed.futureValue
        .getMessage shouldBe "delete 3"
      dao.sequenceNrs shouldBe Seq(1L, 2L, 3L)
    }
  }
}
