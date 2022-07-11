/*
 *  Copyright (c) 2014 by Autodesk, Inc.
 *  All rights reserved.
 *
 *  The information contained herein is confidential and proprietary to
 *  Autodesk, Inc., and considered a trade secret as defined under civil
 *  and criminal statutes.  Autodesk shall pursue its civil and criminal
 *  remedies in the event of unauthorized use or misappropriation of its
 *  trade secrets.  Use of this information by anyone other than authorized
 *  employees of Autodesk, Inc. is granted only under a written non-
 *  disclosure agreement, expressly prescribing the scope and manner of
 *  such use.
 *
 */
package com.autodesk.oss.dealer

import java.io.InputStream
import java.net.{ InetAddress, URI }
import com.autodesk.platform.objectstorage.api.v1.dao._
import com.autodesk.platform.objectstorage.api.v1.dao.providers.DirectToS3Stream.MergeResult
import com.autodesk.platform.objectstorage.api.v1.dao.providers.S3TransferManager
import com.autodesk.platform.objectstorage.api.v1.facets.OSSFacet
import com.autodesk.platform.objectstorage.api.v1.model._
import com.autodesk.platform.objectstorage.api.v1.model.urn.OssObjectUrn
import com.autodesk.platform.objectstorage.util._
import com.autodesk.platform.utils.analytics._
import com.autodesk.platform.utils.json.json4s.{ Json, Json4sApi }
import com.autodesk.platform.utils.logging.{ ContextInfo, UsesLogger }
import com.google.common.hash.Hashing
import com.google.common.io.{ ByteProcessor, ByteSource }
import com.sun.istack.internal.ByteArrayDataSource
import org.joda.time.{ DateTime, Duration => JodaDuration }
import javax.xml.bind.DatatypeConverter

import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.security.MessageDigest
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.concurrent.Future

class Dealer(app: DealerApp, uploadTopicName: String, copyTopicName: String) extends UsesLogger {

  import app._

  lazy val serviceMonikerId = config.getString("oss.moniker.id")
  lazy val appKey = config.getString("oauth.appkey")
  lazy val unreferencedBlobExpirationGrace = JodaDuration.millis(config.getDuration("aws.dynamo.unreferencedBlobExpirationGrace", TimeUnit.MILLISECONDS))

  def start() = {
    def consume[T: Manifest](topic: String)(callback: T => Future[Unit]): Unit = provider.consumer.consumeJsonValuesM[T](topic, provider.getChannelNumStreams(topic))(callback)
    consume[Message[ObjectUploaded]](uploadTopicName)(consumeUploadMessage)
    consume[Message[CopyObjectTo]](copyTopicName)(consumeCopyMessage)
    consume[Message[PotentiallyUnreferencedBlob]](potentiallyUnreferencedBlobTopicName)(consumeBlobNoLongerReferencedMessage)
  }

  def consumeBlobNoLongerReferencedMessage(message: Message[PotentiallyUnreferencedBlob]): Future[Unit] = {
    bucketDao.getBucket(message.entry.bucketKey).flatMap { bucketEntry =>
      if (bucketEntry.isReadOnly) {
        logger warn s"message skipped because the bucket ${message.entry.bucketKey} is in a read-only mode"
        logger warn Json.generate(message)
        Future.successful(())
      } else {
        objectDao.cleanupUnreferencedObjectLocation(message.entry.bucketKey, message.entry.sha1)
      }
    }
  }

  def consumeUploadMessage(message: Message[ObjectUploaded]): Future[Unit] = {
    val startTime = DateTime.now
    val entry = message.entry

    bucketDao.getBucket(entry.obj.bucketKey).flatMap { bucketEntry =>
      if (bucketEntry.isReadOnly) {
        logger warn s"message skipped because the bucket ${message.entry.obj.bucketKey} is in a read-only mode"
        logger warn Json.generate(message)
        Future.successful(())
      } else {
        // the client is temporarily an option for compatibility with previous upload messages that did not include it
        val clientsNotified = (entry.clientNotification fold (Future successful (()))) {
          case (client, notification) =>

            clientQueueMappingDao getQueuesForClientId client map {
              case queues if queues.isEmpty =>
                logger info s"client $client does not need to be notified of upload"
              case queues =>
                logger info s"notifying client $client of finished upload on queues ${queues.mkString("[", ", ", "]")}"
                queues foreach (queue => getProvider(queue._2).producer.sendJson(queue._1, None, notification))
            } recover {
              case e =>
                logger warn (s"failed to notify client $client on file upload $entry", e)
                throw e
            }
        }

        // after (and only if) we've finished notifying clients, start moving the file
        for {
          _ <- clientsNotified
          _ <- entry.obj.blob match {
            case Sha1(sha1) =>
              processCompleteObject(startTime, entry.obj, sha1, entry.policyKey, entry.storageClass)
            case SessionKey(sessionKey) =>
              processResumableSessionObject(startTime, entry.obj, sessionKey, entry.policyKey, entry.storageClass)
            case _: DirectUploadId =>
              // if we ever see a direct upload id we can be certain the entry will have a loc and policy key
              processDirectUpload(startTime, entry.obj, entry.loc.get, entry.policyKey.get, entry.storageClass)

          }
        } yield ()
      }
    }

  }

  def consumeCopyMessage(message: Message[CopyObjectTo]): Future[Unit] = {
    val startTime = DateTime.now
    val entry = message.entry

    bucketDao.getBucket(entry.obj.bucketKey).flatMap { bucketEntry =>
      if (bucketEntry.isReadOnly) {
        logger warn s"message skipped because the bucket ${message.entry.obj.bucketKey} is in a read-only mode"
        logger warn Json.generate(message)
        Future.successful(())
      } else {
        for {
          _ <- entry.obj.blob match {
            case Sha1(sha1) =>
              processCompleteObject(startTime, entry.obj, sha1, entry.policyKey, entry.storageClass)

            case SessionKey(sessionKey) =>
              processResumableSessionObject(startTime, entry.obj, sessionKey, entry.policyKey, entry.storageClass)
          }
        } yield ()
      }
    }

  }

  private def processResumableSessionObject(startTime: DateTime, obj: ObjectEntry, sessionKey: String, policyKey: Option[String], storageClass: String): Future[Unit] =
    resumableObjectStream.downloadFromSession(obj.bucketKey, sessionKey, None)(implicitly, ContextInfo.NoContext).flatMap {
      case None =>
        logger.warn(s"upload of ${obj.bucketKey}/${obj.objectKey} ignored as related session $sessionKey was not found in storage")
        Future.successful(())
      case Some(byteSource) =>
        val stream = byteSource.openBufferedStream()
        val upload = uploadStreamToS3(startTime, obj, sessionKey, stream)
        upload.andThen { case _ => stream.close() } // BucketDoesNotExistException could be thrown in the upload, we need to make sure stream is closed
    }

  def uploadStreamToS3(startTime: DateTime, obj: ObjectEntry, sessionKey: String, stream: InputStream): Future[Unit] =
    policyRetentionCache.getRetentionProperties(obj.bucketKey).flatMap { retProp =>
      directToS3Stream.merge(obj.bucketKey, sessionKey, obj.size, stream).flatMap { merge: MergeResult =>
        val expiration = retProp.newExpirationDate()
        logger.info(s"resumable-v2 finished upload to s3 of session ${obj.bucketKey}/$sessionKey (size ${obj.size}) to temporary destination ${merge.s3Bucket}/${merge.s3Object}."
          + s" will check db for existing blob with calculated sha ${merge.sha1}")
        // file upload succeeded. will try and take the blob if it already exists or insert it otherwise
        objectDao.tryTakeBlob(obj.bucketKey, merge.sha1, expiration).flatMap {

          case Some(existingBlob) =>
            logger.info(s"resumable-v2 detected existing blob for ${obj.bucketKey}/${merge.sha1}."
              + s" will remove uploaded session ${obj.bucketKey}/$sessionKey from temporary destination ${merge.s3Bucket}/${merge.s3Object}")
            // blob does exist. delete the temporary s3 blob and use the existing one
            s3tm.delete(merge.s3Bucket, merge.s3Object).map(_ => existingBlob -> true)
          case None =>
            // blob does not yet exists, move s3 blob to its final destination and insert it
            val newS3Object = location.s3BlobKey(obj.bucketKey, merge.sha1)
            val newUri = location.s3Uri(merge.s3Bucket, newS3Object)

            logger.info(s"resumable-v2 did not found preexisting blob for ${obj.bucketKey}/${merge.sha1} for uploaded session ${obj.bucketKey}/$sessionKey."
              + s" will move to permanent destination ${merge.s3Bucket}/$newS3Object from temporary destination ${merge.s3Bucket}/${merge.s3Object} and insert a new one")

            s3tm.move(merge.s3Bucket, merge.s3Object, newS3Object, retProp.storageClass).flatMap { _ =>
              objectDao.insertBlob(obj.bucketKey, merge.sha1, expiration, newUri.toString, Some(Crypto.MEMORY_PLAIN), obj.size).map(_ -> false)
            }

        }.flatMap {
          case (blob, deduped) =>
            logger.info(s"resumable-v2 will try update the object ${obj.bucketKey}/${obj.objectKey} replacing its session blob ${obj.blob} with its recently calculated sha1 ${blob.sha1}")

            // we got a blob (either existing or new. update the object
            objectDao.assignSha1ToSessionOrDirectUploadObject(obj.bucketKey, obj.objectKey, blob.sha1, SessionKey(sessionKey)).map {
              case Some(updatedObject) =>
                logger.info(s"resumable-v2 finished processing object ${obj.bucketKey}/${obj.objectKey}. session blob ${obj.blob} replaced with its recently calculated sha1 ${blob.sha1}")

                if (updatedObject.isDeleted)
                  app.signalPossibleUnreferencedBlob(obj.bucketKey, blob.sha1)

                analyticsLogger.infoAnalytics(generateResumableSessionObjectAnalyticsLog(startTime, retProp.bucketPolicy, obj, sessionKey, blob.dataLocation, deduped))
              // the object was successfully updated, let's celebrate
              case None =>
                logger.info(s"resumable-v2 stopped processing object ${obj.bucketKey}/${obj.objectKey} as it no longer references the session blob ${obj.blob}. "
                  + s"this probably means a new fresh object was uploaded. will check if recently calculated blob with sha1 ${blob.sha1} is still required or clean it up")
                // after all this effort to transform the session into a proper blob,
                // someone updated the object and no longer references it,
                // which means the fresh new blob might not be needed after all
                app.signalPossibleUnreferencedBlob(obj.bucketKey, blob.sha1)
            }
        }
      }
    }

  def processDirectUpload(startTime: DateTime, obj: ObjectEntry, directBlob: LocationEntry, policyKey: String, storageClass: String): Future[Unit] = {

    def processByteSource(byteSource: ByteSource): (String, Seq[BlockEntry]) = {
      val blockSize: Int = BlockSettings.MINIMUM_BLOCK_SIZE
      def createBlockEntry(blockHash: String, length: Int, blockNumber: Int, obj: ObjectEntry): BlockEntry = {
        BlockEntry(obj.bucketKey, blobHash = "", blockSize, blockNumber, blockHash, length, blockNumber * blockSize, +blockNumber * blockSize + length - 1, obj.createDate, obj.size)
      }

      val sha1Processor: ByteProcessor[(String, Seq[BlockEntry])] = new ByteProcessor[(String, Seq[BlockEntry])] {
        var bytesRead: Int = 0
        val blockDigest256 = MessageDigest.getInstance("SHA-256")
        val fileDigest = MessageDigest.getInstance("SHA-1")
        var blockEntries: Seq[BlockEntry] = Seq[BlockEntry]()

        override def processBytes(buf: Array[Byte], off: Int, len: Int): Boolean = {
          if (bytesRead + len > blockSize) {
            val bytesToWrite = blockSize - bytesRead
            blockDigest256.update(buf, off, bytesToWrite)
            val blockHash = DatatypeConverter.printHexBinary(blockDigest256.digest()).toLowerCase()
            blockEntries = blockEntries :+ createBlockEntry(blockHash, blockSize, blockEntries.size, obj)
            // Start over and read the rest of the buffer.
            bytesRead = len - bytesToWrite
            blockDigest256.update(buf, off + bytesToWrite, bytesRead)
          } else if (bytesRead + len == blockSize) {
            // Somehow we read just the right amount of bytes.
            blockDigest256.update(buf, off, len)
            val blockHash = DatatypeConverter.printHexBinary(blockDigest256.digest()).toLowerCase()
            blockEntries = blockEntries :+ createBlockEntry(blockHash, blockSize, blockEntries.size, obj)
            bytesRead = 0
          } else {
            blockDigest256.update(buf, off, len)
            bytesRead += len
          }
          fileDigest.update(buf, off, len)
          return true
        }

        override def getResult: (String, Seq[BlockEntry]) = {
          if (bytesRead > 0) {
            blockEntries = blockEntries :+ createBlockEntry(DatatypeConverter.printHexBinary(blockDigest256.digest()).toLowerCase(), bytesRead, blockEntries.size, obj)
          }

          // Finish calculating the full file SHA1.
          val fullFileSha1 = DatatypeConverter.printHexBinary(fileDigest.digest()).toLowerCase()
          (fullFileSha1, blockEntries.map(_.copy(blobHash = fullFileSha1)))
        }
      }
      byteSource.read(sha1Processor)
    }

    def calculateHashes(retention: RetentionProperties): Future[(String, Seq[BlockEntry])] = Future {
      logger.info(s"direct upload detected for ${obj.bucketKey}/${obj.objectKey}, will download the blob ${directBlob.dataLocation} to calculate its sha1")
      if (app.blockHashBuckets.contains(obj.bucketKey) && retention.persistent) {
        val byteSource: ByteSource = s3tm.download(directBlob.dataLocation, None, Crypto.transfer(directBlob.crypto))
        val sha1sObject = processByteSource(byteSource: ByteSource)
        logger.info(s"direct upload to ${obj.bucketKey}/${obj.objectKey} downloaded the blob and calculated its sha1 ${sha1sObject._1}")
        sha1sObject
      } else {
        (s3tm.download(directBlob.dataLocation, None, Crypto.transfer(directBlob.crypto)).hash(Hashing.sha1()).toString(), Seq.empty[BlockEntry])
      }
    }

    def takeBlob(sha1: String, expiration: Option[Long]): Future[LocationEntry] = {
      objectDao.tryTakeBlob(obj.bucketKey, sha1, expiration).flatMap {
        case Some(blob) =>
          logger.info(s"direct upload to ${obj.bucketKey}/${obj.objectKey} detected there already existed a blob for its calculated sha1 ${sha1}, will reuse it")
          Future.successful(blob)
        case None =>
          logger.info(s"direct upload to ${obj.bucketKey}/${obj.objectKey} found no preexisting blob for its calculated sha1 ${sha1}, will copy to definitive location and create it")
          val expectedDefinitiveURI = location.s3Uri(s"$s3BucketPrefix-$policyKey", obj.bucketKey, sha1)
          s3tm.copy(directBlob.dataLocation, expectedDefinitiveURI, storageClass).flatMap { _ =>
            objectDao.insertBlob(obj.bucketKey, sha1, expiration, expectedDefinitiveURI.toString, directBlob.crypto, obj.size).map { blob =>
              logger.info(s"direct upload to ${obj.bucketKey}/${obj.objectKey} inserted new blob for its calculated sha1 ${sha1}")
              blob
            }
          }
      }
    }

    def updateObjectWithSha1(sha1: String): Future[Unit] = {
      logger.info(s"direct upload will try update the object ${obj.bucketKey}/${obj.objectKey} replacing its reference to direct upload blob ${obj.blob} with its recently calculated sha1 ${sha1}")
      objectDao.assignSha1ToSessionOrDirectUploadObject(obj.bucketKey, obj.objectKey, sha1, obj.blob).map {
        case Some(blob) =>
          logger.info(s"direct upload finished processing object ${obj.bucketKey}/${obj.objectKey}. updated its reference to direct upload blob ${obj.blob} with its recently calculated sha1 ${sha1}")
        case None =>
          logger.info(s"direct upload stopped processing object ${obj.bucketKey}/${obj.objectKey} as it no longer references the direct upload blob ${obj.blob}. "
            + s"this probably means a new fresh object was uploaded. will check if recently calculated blob with sha1 ${sha1} is still required or clean it up")
          app.signalPossibleUnreferencedBlob(obj.bucketKey, sha1)
      }
    }

    def insertBlockHashes(blockEntries: Seq[BlockEntry]): Future[Unit] =
      if (blockEntries.nonEmpty) {
        blockDao.createBlocks(blockEntries)
      } else {
        Future.successful(())
      }

    def expireDirectUploadBlob(): Future[Unit] = {
      logger.info(s"direct upload to ${obj.bucketKey}/${obj.objectKey} will expire the replaced direct upload blob ${obj.blob} as it will no longer be needed")
      // Sets expiration to a future date so that active signed URLs pointing to the direct upload location will not be cleaned up until after the signed URLs expire.
      objectDao.updateLocationExpirationIfNotModified(directBlob, Some(DateTime.now.plus(unreferencedBlobExpirationGrace).getMillis)).map { _ =>
        logger.info(s"direct upload to ${obj.bucketKey}/${obj.objectKey} expired the replaced direct upload blob ${obj.blob}. gc will take care of it later")
      }
    }

    for {
      retention <- policyRetentionCache.getRetentionProperties(obj.bucketKey)
      (sha1, blockEntries) <- calculateHashes(retention)
      _ <- takeBlob(sha1, retention.newExpirationDate())
      _ <- updateObjectWithSha1(sha1)
      _ <- insertBlockHashes(blockEntries)
      _ <- expireDirectUploadBlob()
    } yield ()
  }.recover {
    case S3TransferManager.Download.NotFound =>
      logger.warn(s"direct upload to ${obj.bucketKey}/${obj.objectKey} did not find the blob ${directBlob.dataLocation} to calculate its sha")
  }

  private def retrievePolicyKey(bucketKey: String, maybePolicyKey: Option[String]): Future[String] =
    maybePolicyKey.fold(bucketDao.getBucket(bucketKey).map(_.policyKey))(Future.successful)

  private def processCompleteObject(startTime: DateTime, obj: ObjectEntry, sha1: String, policyKey: Option[String], storageClass: String): Future[Unit] =
    objectDao.getLocation(obj.bucketKey, sha1) flatMap {
      case None =>
        // location already deleted? some mix-up in the queues? nothing we can do here but log
        logger info s"upload of ${obj.bucketKey}/${obj.objectKey} ignored as related blob ${obj.blob} was not found"
        Future.successful(())

      case Some(loc) =>
        retrievePolicyKey(loc.bucketKey, policyKey).flatMap(handleObjectAlreadyInS3(obj, loc, _, storageClass))

    }

  def handleObjectAlreadyInS3(obj: ObjectEntry, loc: LocationEntry, policyKey: String, storageClass: String): Future[Unit] = {
    val expectedDefinitiveURI = location.s3Uri(s"$s3BucketPrefix-$policyKey", loc.bucketKey, loc.sha1)

    val move = if (loc.dataLocation == expectedDefinitiveURI) {
      //      s3tm.encrypIfNeeded(loc.dataLocation, storageClass) // now unnecessary since there's a story to handle errors in s3 encryption
      logger info s"upload of ${obj.bucketKey}/${obj.objectKey} ignored as related blob ${obj.blob} is already in s3"
      // nothing to do here
      Future.successful(false)
    } else {
      logger info s"upload of ${obj.bucketKey}/${obj.objectKey} found object in nonstandard s3 location. dealer will move it"
      s3tm.copy(loc.dataLocation, expectedDefinitiveURI, storageClass).flatMap { _ =>
        update(obj, expectedDefinitiveURI).flatMap { updated =>
          if (updated) markForCleanUp(loc) else Future.successful(false)
        }
      }
    }
    move.flatMap(_ => Future.successful(()))
  }

  private def update(obj: ObjectEntry, s3Uri: URI): Future[Boolean] = {
    objectDao.updateDataLocation(obj.bucketKey, obj.blob.toString, s3Uri.toString) map { _ =>
      logger info s"updated blob record for ${obj.blob} set location to $s3Uri"
      true
    } recover {
      case e =>
        logger warn (s"failed to update blob record for ${obj.blob}. exception ${e.getClass} ${e.getMessage}", e)
        false
    }
  }

  def delete(loc: URI): Future[Boolean] =
    if (loc.getScheme.equalsIgnoreCase("gfs"))
      Future { true }
    else s3tm.delete(loc).map(_ => true).recover {
      case e =>
        // stack trace here is not normally needed
        logger info s"failed to delete blob $loc caught exception ${e.getClass} ${e.getMessage}"
        false
    }

  private def markForCleanUp(loc: LocationEntry) = {
    objectDao.insertCleanUpPlaceholder(loc) map { _ =>
      logger.info(s"inserted placeholder blob entry for clean up of ${loc.dataLocation.toString}")
      true
    } recover {
      case e =>
        logger.warn(s"failed to insert placeholder entry for blob clean up of S${loc.dataLocation.toString}")
        false
    }
  }

  def generateCompleteObjectAnalyticsLog(startTime: DateTime, bucketPolicy: String, obj: ObjectEntry, from: URI, to: URI): String =
    generateAnalyticsLog("move", startTime, bucketPolicy, obj, Some(from), to, None, None)

  def generateResumableSessionObjectAnalyticsLog(startTime: DateTime, bucketPolicy: String, obj: ObjectEntry, sessionId: String, to: URI, deduped: Boolean): String =
    generateAnalyticsLog("complete_session", startTime, bucketPolicy, obj, None, to, Some(sessionId), Some(deduped))

  def generateAnalyticsLog(action: String, startTime: DateTime, bucketPolicy: String, obj: ObjectEntry, from: Option[URI], to: URI, sessionId: Option[String], deduped: Option[Boolean]): String = {
    val f = OSSFacet(
      version = version,
      api_type = "dealer",
      action_type = action,
      urn = Some(OssObjectUrn(obj.bucketKey, obj.objectKey).toString()),
      bucket_policy = Some(bucketPolicy),
      bucket_key = Some(obj.bucketKey),
      object_key = Some(obj.objectKey),
      object_size = Some(obj.size),
      sha1 = obj.blob.maybeSha1,
      content_type = obj.`content-type`,
      move_from = from.map(_.toString),
      move_to = Some(to.toString),
      session_id = sessionId,
      object_deduped = deduped.map(if (_) 1 else 0)
    )

    val logEntry = AnalyticsLogEntry(
      start_time = startTime,
      duration = DateTime.now().getMillis - startTime.getMillis,
      status = "ok",
      context_call = action,
      context_identity = "Unavailable",
      context_tenant = None,
      context_user = None,
      context_session = None,
      context_job = None,
      source_service = name,
      consumer_src = "Unavailable",
      api_category = "Dealer",
      api_scope = "F",
      api_level = "internal",
      api_method = action,
      facets = Seq(f),
      version = "1.0.0",
      origin_server = InetAddress.getLocalHost.getHostName
    )
    jsonSerializer.generate(logEntry)
  }

  // add this here for the moment, we'll rewrite the whole analytics stuff later
  val jsonSerializer = new Json4sApi {
    override def formats = super.formats + AnalyticsLogEntryJsonSerializer
  }

}
