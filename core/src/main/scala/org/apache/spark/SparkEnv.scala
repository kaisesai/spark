/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File

import scala.collection.concurrent
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Properties

import com.google.common.base.Preconditions
import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.api.python.{PythonWorker, PythonWorkerFactory}
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKeys
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.{MetricsSystem, MetricsSystemInstances}
import org.apache.spark.network.netty.{NettyBlockTransferService, SparkTransportConf}
import org.apache.spark.network.shuffle.ExternalBlockStoreClient
import org.apache.spark.rpc.{RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.scheduler.OutputCommitCoordinator.OutputCommitCoordinatorEndpoint
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.serializer.{JavaSerializer, Serializer, SerializerManager}
import org.apache.spark.shuffle.ShuffleManager
import org.apache.spark.storage._
import org.apache.spark.util.{RpcUtils, Utils}
import org.apache.spark.util.ArrayImplicits._

/**
 * :: DeveloperApi ::
 * Holds all the runtime environment objects for a running Spark instance (either master or worker),
 * including the serializer, RpcEnv, block manager, map output tracker, etc. Currently
 * Spark code finds the SparkEnv through a global variable, so all the threads can access the same
 * SparkEnv. It can be accessed by SparkEnv.get (e.g. after creating a SparkContext).
 */
@DeveloperApi
class SparkEnv (
    val executorId: String,
    private[spark] val rpcEnv: RpcEnv,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val serializerManager: SerializerManager,
    val mapOutputTracker: MapOutputTracker,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val securityManager: SecurityManager,
    val metricsSystem: MetricsSystem,
    val outputCommitCoordinator: OutputCommitCoordinator,
    val conf: SparkConf) extends Logging {

  // We initialize the ShuffleManager later in SparkContext and Executor to allow
  // user jars to define custom ShuffleManagers.
  @volatile private var _shuffleManager: ShuffleManager = _

  // 后续的 shuffle manager
  def shuffleManager: ShuffleManager = _shuffleManager

  // We initialize the MemoryManager later in SparkContext after DriverPlugin is loaded
  // to allow the plugin to overwrite executor memory configurations
  private var _memoryManager: MemoryManager = _

  def memoryManager: MemoryManager = _memoryManager

  @volatile private[spark] var isStopped = false

  /**
   * A key for PythonWorkerFactory cache.
   * @param pythonExec The python executable to run the Python worker.
   * @param workerModule The worker module to be called in the worker, e.g., "pyspark.worker".
   * @param daemonModule The daemon module name to reuse the worker, e.g., "pyspark.daemon".
   * @param envVars The environment variables for the worker.
   */
  private case class PythonWorkersKey(
      pythonExec: String, workerModule: String, daemonModule: String, envVars: Map[String, String])
  private val pythonWorkers = mutable.HashMap[PythonWorkersKey, PythonWorkerFactory]()

  // A general, soft-reference map for metadata needed during HadoopRDD split computation
  // (e.g., HadoopFileRDD uses this to cache JobConfs and InputFormats).
  private[spark] val hadoopJobMetadata =
    CacheBuilder.newBuilder().maximumSize(1000).softValues().build[String, AnyRef]().asMap()

  private[spark] var driverTmpDir: Option[String] = None

  private[spark] var executorBackend: Option[ExecutorBackend] = None

  private[spark] def stop(): Unit = {

    if (!isStopped) {
      isStopped = true
      pythonWorkers.values.foreach(_.stop())
      mapOutputTracker.stop()
      if (shuffleManager != null) {
        shuffleManager.stop()
      }
      broadcastManager.stop()
      blockManager.stop()
      blockManager.master.stop()
      metricsSystem.stop()
      outputCommitCoordinator.stop()
      rpcEnv.shutdown()
      rpcEnv.awaitTermination()

      // If we only stop sc, but the driver process still run as a services then we need to delete
      // the tmp dir, if not, it will create too many tmp dirs.
      // We only need to delete the tmp dir create by driver
      driverTmpDir match {
        case Some(path) =>
          try {
            Utils.deleteRecursively(new File(path))
          } catch {
            case e: Exception =>
              logWarning(log"Exception while deleting Spark temp dir: " +
                log"${MDC(LogKeys.PATH, path)}", e)
          }
        case None => // We just need to delete tmp dir created by driver, so do nothing on executor
      }
    }
  }

  private[spark] def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      useDaemon: Boolean): (PythonWorker, Option[Int]) = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      val workerFactory = pythonWorkers.getOrElseUpdate(key, new PythonWorkerFactory(
          pythonExec, workerModule, daemonModule, envVars, useDaemon))
      if (workerFactory.useDaemonEnabled != useDaemon) {
        throw SparkException.internalError("PythonWorkerFactory is already created with " +
          s"useDaemon = ${workerFactory.useDaemonEnabled}, but now is requested with " +
          s"useDaemon = $useDaemon. This is not allowed to change after the PythonWorkerFactory " +
          s"is created given the same key: $key.")
      }
      workerFactory.create()
    }
  }

  private[spark] def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      useDaemon: Boolean): (PythonWorker, Option[Int]) = {
    createPythonWorker(
      pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule, envVars, useDaemon)
  }

  private[spark] def createPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String]): (PythonWorker, Option[Int]) = {
    val useDaemon = conf.get(Python.PYTHON_USE_DAEMON)
    createPythonWorker(
      pythonExec, workerModule, daemonModule, envVars, useDaemon)
  }

  private[spark] def destroyPythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      pythonWorkers.get(key).foreach(_.stopWorker(worker))
    }
  }

  private[spark] def destroyPythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    destroyPythonWorker(
      pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule, envVars, worker)
  }

  private[spark] def releasePythonWorker(
      pythonExec: String,
      workerModule: String,
      daemonModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    synchronized {
      val key = PythonWorkersKey(pythonExec, workerModule, daemonModule, envVars)
      pythonWorkers.get(key).foreach(_.releaseWorker(worker))
    }
  }

  private[spark] def releasePythonWorker(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      worker: PythonWorker): Unit = {
    releasePythonWorker(
      pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule, envVars, worker)
  }
  /**
   * 初始化 shuffle manager
   */
  private[spark] def initializeShuffleManager(): Unit = {
    Preconditions.checkState(null == _shuffleManager,
      "Shuffle manager already initialized to %s", _shuffleManager)
    // shuffle 管理器
    _shuffleManager = ShuffleManager.create(conf, executorId == SparkContext.DRIVER_IDENTIFIER)
  }

  private[spark] def initializeMemoryManager(numUsableCores: Int): Unit = {
    Preconditions.checkState(null == memoryManager,
      "Memory manager already initialized to %s", _memoryManager)
    // 统一内存管理器
    _memoryManager = UnifiedMemoryManager(conf, numUsableCores)
  }
}

object SparkEnv extends Logging {
  @volatile private var env: SparkEnv = _

  private[spark] val driverSystemName = "sparkDriver"
  private[spark] val executorSystemName = "sparkExecutor"

  def set(e: SparkEnv): Unit = {
    env = e
  }

  /**
   * Returns the SparkEnv.
   */
  def get: SparkEnv = {
    env
  }

  // 创建 driver 环境下 spark env
  /**
   * Create a SparkEnv for the driver.
   */
  private[spark] def createDriverEnv(
      conf: SparkConf,
      isLocal: Boolean,
      listenerBus: LiveListenerBus,
      numCores: Int,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {
    assert(conf.contains(DRIVER_HOST_ADDRESS),
      s"${DRIVER_HOST_ADDRESS.key} is not set on the driver!")
    assert(conf.contains(DRIVER_PORT), s"${DRIVER_PORT.key} is not set on the driver!")
    // 驱动绑定的地址
    val bindAddress = conf.get(DRIVER_BIND_ADDRESS)
    // 驱动地址,通告地址
    val advertiseAddress = conf.get(DRIVER_HOST_ADDRESS)
    // 端口
    val port = conf.get(DRIVER_PORT)
    val ioEncryptionKey = if (conf.get(IO_ENCRYPTION_ENABLED)) {
      Some(CryptoStreamUtils.createKey(conf))
    } else {
      None
    }
    // 创建 SparkEnv
    create(
      conf,
      SparkContext.DRIVER_IDENTIFIER,
      bindAddress,
      advertiseAddress,
      Option(port),
      isLocal,
      numCores,
      ioEncryptionKey,
      listenerBus = listenerBus,
      mockOutputCommitCoordinator = mockOutputCommitCoordinator
    )
  }

  /**
   * Create a SparkEnv for an executor.
   * In coarse-grained mode, the executor provides an RpcEnv that is already instantiated.
   */
  private[spark] def createExecutorEnv(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      hostname: String,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {
    // 创建 executorEnv
    val env = create(
      conf,
      executorId,
      bindAddress,
      hostname,
      None,
      isLocal,
      numCores,
      ioEncryptionKey
    )
    // Set the memory manager since it needs to be initialized explicitly
    env.initializeMemoryManager(numCores)
    SparkEnv.set(env)
    env
  }

  /**
   * Helper method to create a SparkEnv for a driver or an executor.
   *
   * 实际的创建 spark env 的核心方法
   */
  private def create(
      conf: SparkConf,
      executorId: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Option[Int],
      isLocal: Boolean,
      numUsableCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      listenerBus: LiveListenerBus = null,
      mockOutputCommitCoordinator: Option[OutputCommitCoordinator] = None): SparkEnv = {

    // 是否驱动
    val isDriver = executorId == SparkContext.DRIVER_IDENTIFIER

    // Listener bus is only used on the driver
    if (isDriver) {
      assert(listenerBus != null, "Attempted to create driver SparkEnv with null listener bus!")
    }
    val authSecretFileConf = if (isDriver) AUTH_SECRET_FILE_DRIVER else AUTH_SECRET_FILE_EXECUTOR
    // 安全管理器
    val securityManager = new SecurityManager(conf, ioEncryptionKey, authSecretFileConf)
    if (isDriver) {
      securityManager.initializeAuth()
    }

    ioEncryptionKey.foreach { _ =>
      if (!(securityManager.isEncryptionEnabled() || securityManager.isSslRpcEnabled())) {
        logWarning("I/O encryption enabled without RPC encryption: keys will be visible on the " +
          "wire.")
      }
    }

    val systemName = if (isDriver) driverSystemName else executorSystemName
    // 创建一个 netty服务
    val rpcEnv = RpcEnv.create(systemName, bindAddress, advertiseAddress, port.getOrElse(-1), conf,
      securityManager, numUsableCores, !isDriver)

    // Figure out which port RpcEnv actually bound to in case the original port is 0 or occupied.
    if (isDriver) {
      conf.set(DRIVER_PORT, rpcEnv.address.port)
    }

    // 序列化器
    val serializer = Utils.instantiateSerializerFromConf[Serializer](SERIALIZER, conf, isDriver)
    logDebug(s"Using serializer: ${serializer.getClass}")

    // 序列化器管理器
    val serializerManager = new SerializerManager(serializer, conf, ioEncryptionKey)

    // 闭包序列化
    val closureSerializer = new JavaSerializer(conf)

    // 注册以及寻找端点
    def registerOrLookupEndpoint(
        name: String, endpointCreator: => RpcEndpoint):
      // 端点引用
      RpcEndpointRef = {
      if (isDriver) {
        logInfo(log"Registering ${MDC(LogKeys.ENDPOINT_NAME, name)}")
        rpcEnv.setupEndpoint(name, endpointCreator)
      } else {
        RpcUtils.makeDriverRef(name, conf, rpcEnv)
      }
    }

    // 广播管理器
    val broadcastManager = new BroadcastManager(isDriver, conf)

    // map 输出记录器
    val mapOutputTracker = if (isDriver) {
      new MapOutputTrackerMaster(conf, broadcastManager, isLocal)
    } else {
      new MapOutputTrackerWorker(conf)
    }

    // Have to assign trackerEndpoint after initialization as MapOutputTrackerEndpoint
    // requires the MapOutputTracker itself
    mapOutputTracker.trackerEndpoint = registerOrLookupEndpoint(MapOutputTracker.ENDPOINT_NAME,
      // 映射输出轨迹 tracker 端点
      new MapOutputTrackerMasterEndpoint(
        rpcEnv, mapOutputTracker.asInstanceOf[MapOutputTrackerMaster], conf))

    val blockManagerPort = if (isDriver) {
      conf.get(DRIVER_BLOCK_MANAGER_PORT)
    } else {
      conf.get(BLOCK_MANAGER_PORT)
    }

    // 扩展的shuffle客户端
    val externalShuffleClient = if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
      val transConf = SparkTransportConf.fromSparkConf(
        conf,
        "shuffle",
        numUsableCores,
        sslOptions = Some(securityManager.getRpcSSLOptions())
      )
      // 扩展的块存储客户端
      Some(new ExternalBlockStoreClient(transConf, securityManager,
        securityManager.isAuthenticationEnabled(), conf.get(config.SHUFFLE_REGISTRATION_TIMEOUT)))
    } else {
      None
    }

    // Mapping from block manager id to the block manager's information.
    val blockManagerInfo = new concurrent.TrieMap[BlockManagerId, BlockManagerInfo]()

    // 块管理器 master (driver端会创建, 其他端引用它的ref)
    val blockManagerMaster = new BlockManagerMaster(
      // 注册端点: 阻塞管理器master端点
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_ENDPOINT_NAME,
        // 阻塞管理器master端点
        new BlockManagerMasterEndpoint(
          rpcEnv,
          isLocal,
          conf,
          listenerBus,
          if (conf.get(config.SHUFFLE_SERVICE_ENABLED)) {
            externalShuffleClient
          } else {
            None
          }, blockManagerInfo,
          mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
          _shuffleManager = null,
          isDriver)),
      // 注册端点: 心跳端点
      registerOrLookupEndpoint(
        BlockManagerMaster.DRIVER_HEARTBEAT_ENDPOINT_NAME,
        new BlockManagerMasterHeartbeatEndpoint(rpcEnv, isLocal, blockManagerInfo)),
      conf,
      isDriver)

    // 块传输服务
    val blockTransferService =
      new NettyBlockTransferService(conf, securityManager, serializerManager, bindAddress,
        advertiseAddress, blockManagerPort, numUsableCores, blockManagerMaster.driverEndpoint)

    // NB: blockManager is not valid until initialize() is called later.
    //     SPARK-45762 introduces a change where the ShuffleManager is initialized later
    //     in the SparkContext and Executor, to allow for custom ShuffleManagers defined
    //     in user jars. The BlockManager uses a lazy val to obtain the
    //     shuffleManager from the SparkEnv.
    // 块管理器
    val blockManager = new BlockManager(
      executorId,
      rpcEnv,
      blockManagerMaster,
      serializerManager,
      conf,
      _memoryManager = null,
      mapOutputTracker,
      _shuffleManager = null, // shuffle 管理器
      blockTransferService,
      securityManager,
      externalShuffleClient)

    val metricsSystem = if (isDriver) {
      // Don't start metrics system right now for Driver.
      // We need to wait for the task scheduler to give us an app ID.
      // Then we can start the metrics system.
      MetricsSystem.createMetricsSystem(MetricsSystemInstances.DRIVER, conf)
    } else {
      // We need to set the executor ID before the MetricsSystem is created because sources and
      // sinks specified in the metrics configuration file will want to incorporate this executor's
      // ID into the metrics they report.
      conf.set(EXECUTOR_ID, executorId)
      val ms = MetricsSystem.createMetricsSystem(MetricsSystemInstances.EXECUTOR, conf)
      ms.start(conf.get(METRICS_STATIC_SOURCES_ENABLED))
      ms
    }

    // 输出提交协调器
    val outputCommitCoordinator = mockOutputCommitCoordinator.getOrElse {
      new OutputCommitCoordinator(conf, isDriver)
    }
    // 输出提交协调器引用
    val outputCommitCoordinatorRef = registerOrLookupEndpoint("OutputCommitCoordinator",
      new OutputCommitCoordinatorEndpoint(rpcEnv, outputCommitCoordinator))
    outputCommitCoordinator.coordinatorRef = Some(outputCommitCoordinatorRef)

    // spark环境实例
    val envInstance = new SparkEnv(
      executorId,
      rpcEnv,
      serializer,
      closureSerializer,
      serializerManager,
      mapOutputTracker,
      broadcastManager,
      blockManager,
      securityManager,
      metricsSystem,
      outputCommitCoordinator,
      conf)

    // Add a reference to tmp dir created by driver, we will delete this tmp dir when stop() is
    // called, and we only need to do it for driver. Because driver may run as a service, and if we
    // don't delete this tmp dir when sc is stopped, then will create too many tmp dirs.
    if (isDriver) {
      val sparkFilesDir = Utils.createTempDir(Utils.getLocalDir(conf), "userFiles").getAbsolutePath
      envInstance.driverTmpDir = Some(sparkFilesDir)
    }

    // 返回 spark env 实例
    envInstance
  }

  /**
   * Return a map representation of jvm information, Spark properties, system properties, and
   * class paths. Map keys define the category, and map values represent the corresponding
   * attributes as a sequence of KV pairs. This is used mainly for SparkListenerEnvironmentUpdate.
   */
  private[spark] def environmentDetails(
      conf: SparkConf,
      hadoopConf: Configuration,
      schedulingMode: String,
      addedJars: Seq[String],
      addedFiles: Seq[String],
      addedArchives: Seq[String],
      metricsProperties: Map[String, String]): Map[String, Seq[(String, String)]] = {

    import Properties._
    val jvmInformation = Seq(
      ("Java Version", s"$javaVersion ($javaVendor)"),
      ("Java Home", javaHome),
      ("Scala Version", versionString)
    ).sorted

    // Spark properties
    // This includes the scheduling mode whether or not it is configured (used by SparkUI)
    val schedulerMode =
      if (!conf.contains(SCHEDULER_MODE)) {
        Seq((SCHEDULER_MODE.key, schedulingMode))
      } else {
        Seq.empty[(String, String)]
      }
    val sparkProperties = (conf.getAll ++ schedulerMode).sorted

    // System properties that are not java classpaths
    val systemProperties = Utils.getSystemProperties.toSeq
    val otherProperties = systemProperties.filter { case (k, _) =>
      k != "java.class.path" && !k.startsWith("spark.")
    }.sorted

    // Class paths including all added jars and files
    val classPathEntries = javaClassPath
      .split(File.pathSeparator)
      .filterNot(_.isEmpty)
      .map((_, "System Classpath"))
    val addedJarsAndFiles = (addedJars ++ addedFiles ++ addedArchives).map((_, "Added By User"))
    val classPaths = (addedJarsAndFiles ++ classPathEntries).sorted

    // Add Hadoop properties, it will not ignore configs including in Spark. Some spark
    // conf starting with "spark.hadoop" may overwrite it.
    val hadoopProperties = hadoopConf.asScala
      .map(entry => (entry.getKey, entry.getValue)).toSeq.sorted
    Map[String, Seq[(String, String)]](
      "JVM Information" -> jvmInformation,
      "Spark Properties" -> sparkProperties.toImmutableArraySeq,
      "Hadoop Properties" -> hadoopProperties,
      "System Properties" -> otherProperties,
      "Classpath Entries" -> classPaths,
      "Metrics Properties" -> metricsProperties.toSeq.sorted)
  }
}
