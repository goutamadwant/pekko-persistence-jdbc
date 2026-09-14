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

package org.apache.pekko.persistence.jdbc.snapshot

import java.util.concurrent.CopyOnWriteArrayList

import scala.concurrent.{ ExecutionContext, Future }
import scala.jdk.CollectionConverters._

import org.apache.pekko
import pekko.actor.ActorRef
import pekko.persistence.{ DeleteSnapshotsSuccess, Persistence, SelectedSnapshot, SnapshotMetadata }
import pekko.persistence.SnapshotProtocol.{ DeleteSnapshots, LoadSnapshot, LoadSnapshotResult }
import pekko.persistence.SnapshotSelectionCriteria
import pekko.persistence.jdbc.SingleActorSystemPerTestSpec
import pekko.persistence.jdbc.config.SnapshotConfig
import pekko.persistence.jdbc.snapshot.dao.DefaultSnapshotDao
import pekko.persistence.jdbc.testkit.internal.H2
import pekko.serialization.Serialization
import pekko.stream.Materializer
import pekko.testkit.TestProbe
import com.typesafe.config.ConfigValueFactory
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.JdbcProfile

/**
 * A snapshot dao that overrides the criteria methods and records the criteria the plugin passes to them, so that a
 * test can check that the plugin consults a custom dao's overrides rather than the trait defaults.
 */
class RecordingSnapshotDao(
    db: Database,
    profile: JdbcProfile,
    snapshotConfig: SnapshotConfig,
    serialization: Serialization)(implicit ec: ExecutionContext, mat: Materializer)
    extends DefaultSnapshotDao(db, profile, snapshotConfig, serialization) {
  RecordingSnapshotDao.instances.add(this)

  val calls = new CopyOnWriteArrayList[(String, Any)]

  override def snapshotForCriteria(persistenceId: String, criteria: SnapshotSelectionCriteria) = {
    calls.add(("snapshotForCriteria", criteria))
    super.snapshotForCriteria(persistenceId, criteria)
  }

  override def deleteByCriteria(persistenceId: String, criteria: SnapshotSelectionCriteria) = {
    calls.add(("deleteByCriteria", criteria))
    if (RecordingSnapshotDao.skipDeletes) Future.unit else super.deleteByCriteria(persistenceId, criteria)
  }

  // The building blocks of the trait's default deleteByCriteria, recorded to show it is not used.
  override def snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId: String, sequenceNr: Long, timestamp: Long) = {
    calls.add(("snapshotForMaxSequenceNrAndMaxTimestamp", (sequenceNr, timestamp)))
    super.snapshotForMaxSequenceNrAndMaxTimestamp(persistenceId, sequenceNr, timestamp)
  }

  override def delete(persistenceId: String, sequenceNr: Long) = {
    calls.add(("delete", sequenceNr))
    super.delete(persistenceId, sequenceNr)
  }
}

object RecordingSnapshotDao {
  val instances = new CopyOnWriteArrayList[RecordingSnapshotDao]

  /** When true, the deleteByCriteria override records the request and deletes nothing. */
  @volatile var skipDeletes: Boolean = false

  def clear(): Unit = {
    instances.clear()
    skipDeletes = false
  }
}

class JdbcSnapshotStoreCustomDaoSpec
    extends SingleActorSystemPerTestSpec(
      "h2-shared-db-application.conf",
      Map("jdbc-snapshot-store.dao" -> ConfigValueFactory.fromAnyRef(classOf[RecordingSnapshotDao].getName))) {
  private val persistenceId = "custom-dao"
  private val boundedDelete = SnapshotSelectionCriteria(maxSequenceNr = 4, minSequenceNr = 2)
  private val boundedLoad = SnapshotSelectionCriteria(maxSequenceNr = 4, minSequenceNr = 3)

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    RecordingSnapshotDao.clear()
    dropAndCreate(H2)
  }

  private def withSnapshotStore(f: (ActorRef, RecordingSnapshotDao, TestProbe) => Unit): Unit =
    withActorSystem { implicit system =>
      val snapshotStore = Persistence(system).snapshotStoreFor("jdbc-snapshot-store")
      // the plugin actor, and with it the dao, is created asynchronously
      val dao = eventually {
        val daos = RecordingSnapshotDao.instances.asScala.toList
        daos should have size 1
        daos.head
      }
      (1L to 5L).foreach { sequenceNr =>
        dao.save(SnapshotMetadata(persistenceId, sequenceNr, sequenceNr * 100), s"snapshot-$sequenceNr").futureValue
      }
      dao.calls.clear()
      f(snapshotStore, dao, TestProbe())
    }

  private def loaded(snapshotStore: ActorRef, probe: TestProbe): Option[SelectedSnapshot] = {
    snapshotStore.tell(LoadSnapshot(persistenceId, SnapshotSelectionCriteria.Latest, Long.MaxValue), probe.ref)
    probe.expectMsgType[LoadSnapshotResult].snapshot
  }

  it should "pass bounded load criteria to the custom dao's snapshotForCriteria override" in {
    withSnapshotStore { (snapshotStore, dao, probe) =>
      snapshotStore.tell(LoadSnapshot(persistenceId, boundedLoad, Long.MaxValue), probe.ref)
      probe.expectMsg(
        LoadSnapshotResult(Some(SelectedSnapshot(SnapshotMetadata(persistenceId, 4, 400), "snapshot-4")),
          Long.MaxValue))
      dao.calls.asScala.toList shouldBe List(("snapshotForCriteria", boundedLoad))
    }
  }

  it should "pass bounded delete criteria to the custom dao's deleteByCriteria override" in {
    withSnapshotStore { (snapshotStore, dao, probe) =>
      snapshotStore.tell(DeleteSnapshots(persistenceId, boundedDelete), probe.ref)
      probe.expectMsg(DeleteSnapshotsSuccess(boundedDelete))
      dao.calls.asScala.toList shouldBe List(("deleteByCriteria", boundedDelete))
      loaded(snapshotStore, probe).map(_.metadata.sequenceNr) shouldBe Some(5L)
      snapshotStore.tell(LoadSnapshot(persistenceId, SnapshotSelectionCriteria(maxSequenceNr = 4), Long.MaxValue),
        probe.ref)
      probe.expectMsgType[LoadSnapshotResult].snapshot.map(_.metadata.sequenceNr) shouldBe Some(1L)
    }
  }

  it should "use only the custom dao's deleteByCriteria override and never the trait default" in {
    RecordingSnapshotDao.skipDeletes = true
    withSnapshotStore { (snapshotStore, dao, probe) =>
      snapshotStore.tell(DeleteSnapshots(persistenceId, boundedDelete), probe.ref)
      probe.expectMsg(DeleteSnapshotsSuccess(boundedDelete))
      dao.calls.asScala.toList shouldBe List(("deleteByCriteria", boundedDelete))
      dao.calls.clear()
      snapshotStore.tell(LoadSnapshot(persistenceId, SnapshotSelectionCriteria(maxSequenceNr = 4), Long.MaxValue),
        probe.ref)
      probe.expectMsgType[LoadSnapshotResult].snapshot.map(_.metadata.sequenceNr) shouldBe Some(4L)
    }
  }
}
