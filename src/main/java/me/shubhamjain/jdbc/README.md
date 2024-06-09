## Iceberg Duplication
Iceberg Table gets duplicated if we run the flink job again.
There is no way to deduplicate data on batch job.
I tried using ENFORCED mode on primary keys in ICEBERG table.
```shell
2157 [main] INFO org.apache.iceberg.CatalogUtil - Loading custom FileIO implementation: org.apache.iceberg.hadoop.HadoopFileIO
Exception in thread "main" org.apache.flink.sql.parser.error.SqlValidateException: Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode
```

## Good Compression
99 MB sql script is converted to 8 files each of 300KB.
888 MB sql script is converted to 8 files each of ~3MB.
With Finite Source, Job finishes pretty fast.

## Partitioning
Requires some extra memory to run the job.
```shell
/tmp/iceberg/warehouse/default_database/bookings_sink/data/book_date=2017-04-16T13%3A57
```
### Errors due to partitioning
```shell
37090 [flink-pekko.actor.default-dispatcher-8] INFO org.apache.flink.runtime.executiongraph.ExecutionGraph - Source: bookings_source[1] -> SinkConversion[2] (1/8) (c96123741903ce0b871bca3ff2b2099c_cbc357ccb763df2852fee8c4fc7d55f2_0_0) switched from RUNNING to FAILED on a0af0089-d151-4848-92e3-b7231ba4e576 @ localhost (dataPort=-1).
java.lang.OutOfMemoryError: Java heap space
37092 [Flink-DispatcherRestEndpoint-thread-2] ERROR org.apache.flink.util.FatalExitExceptionHandler - Thread dump: 
"main" prio=5 Id=1 WAITING on java.util.concurrent.CompletableFuture$Signaller@6ca37bbc
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on java.util.concurrent.CompletableFuture$Signaller@6ca37bbc
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.park(LockSupport.java:211)
	at java.base@17.0.7/java.util.concurrent.CompletableFuture$Signaller.block(CompletableFuture.java:1864)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.unmanagedBlock(ForkJoinPool.java:3463)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.managedBlock(ForkJoinPool.java:3434)
	at java.base@17.0.7/java.util.concurrent.CompletableFuture.waitingGet(CompletableFuture.java:1898)
	at java.base@17.0.7/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2072)
	at app//org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.execute(StreamExecutionEnvironment.java:2131)
	...

"Reference Handler" daemon prio=10 Id=2 RUNNABLE
	at java.base@17.0.7/java.lang.ref.Reference.waitForReferencePendingList(Native Method)
	at java.base@17.0.7/java.lang.ref.Reference.processPendingReferences(Reference.java:253)
	at java.base@17.0.7/java.lang.ref.Reference$ReferenceHandler.run(Reference.java:215)

"Finalizer" daemon prio=8 Id=3 WAITING on java.lang.ref.ReferenceQueue$Lock@4d644611
	at java.base@17.0.7/java.lang.Object.wait(Native Method)
	-  waiting on java.lang.ref.ReferenceQueue$Lock@4d644611
	at java.base@17.0.7/java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:155)
	at java.base@17.0.7/java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:176)
	at java.base@17.0.7/java.lang.ref.Finalizer$FinalizerThread.run(Finalizer.java:172)

"Signal Dispatcher" daemon prio=9 Id=4 RUNNABLE

"Common-Cleaner" daemon prio=8 Id=12 TIMED_WAITING on java.lang.ref.ReferenceQueue$Lock@2bca3ad
	at java.base@17.0.7/java.lang.Object.wait(Native Method)
	-  waiting on java.lang.ref.ReferenceQueue$Lock@2bca3ad
	at java.base@17.0.7/java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:155)
	at java.base@17.0.7/jdk.internal.ref.CleanerImpl.run(CleanerImpl.java:140)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)
	at java.base@17.0.7/jdk.internal.misc.InnocuousThread.run(InnocuousThread.java:162)

"Monitor Ctrl-Break" daemon prio=5 Id=13 RUNNABLE (in native)
	at java.base@17.0.7/sun.nio.ch.SocketDispatcher.read0(Native Method)
	at java.base@17.0.7/sun.nio.ch.SocketDispatcher.read(SocketDispatcher.java:47)
	at java.base@17.0.7/sun.nio.ch.NioSocketImpl.tryRead(NioSocketImpl.java:261)
	at java.base@17.0.7/sun.nio.ch.NioSocketImpl.implRead(NioSocketImpl.java:312)
	at java.base@17.0.7/sun.nio.ch.NioSocketImpl.read(NioSocketImpl.java:350)
	at java.base@17.0.7/sun.nio.ch.NioSocketImpl$1.read(NioSocketImpl.java:803)
	at java.base@17.0.7/java.net.Socket$SocketInputStream.read(Socket.java:976)
	at java.base@17.0.7/sun.nio.cs.StreamDecoder.readBytes(StreamDecoder.java:270)
	...

	Number of locked synchronizers = 1
	- java.util.concurrent.locks.ReentrantLock$NonfairSync@723dfe97

"Notification Thread" daemon prio=9 Id=14 RUNNABLE

"org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner" daemon prio=5 Id=16 WAITING on java.lang.ref.ReferenceQueue$Lock@1d066a0f
	at java.base@17.0.7/java.lang.Object.wait(Native Method)
	-  waiting on java.lang.ref.ReferenceQueue$Lock@1d066a0f
	at java.base@17.0.7/java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:155)
	at java.base@17.0.7/java.lang.ref.ReferenceQueue.remove(ReferenceQueue.java:176)
	at app//org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner.run(FileSystem.java:3212)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"process reaper" daemon prio=10 Id=18 TIMED_WAITING on java.util.concurrent.SynchronousQueue$TransferStack@7a4003c9
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on java.util.concurrent.SynchronousQueue$TransferStack@7a4003c9
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:252)
	at java.base@17.0.7/java.util.concurrent.SynchronousQueue$TransferStack.transfer(SynchronousQueue.java:401)
	at java.base@17.0.7/java.util.concurrent.SynchronousQueue.poll(SynchronousQueue.java:903)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:1061)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1122)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"ForkJoinPool.commonPool-worker-1" daemon prio=5 Id=26 WAITING on java.util.concurrent.ForkJoinPool@1bf9353c
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on java.util.concurrent.ForkJoinPool@1bf9353c
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.park(LockSupport.java:341)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.awaitWork(ForkJoinPool.java:1724)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1623)
	at java.base@17.0.7/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

"flink-scheduler-1" prio=5 Id=34 TIMED_WAITING
	at java.base@17.0.7/java.lang.Thread.sleep(Native Method)
	at org.apache.pekko.actor.LightArrayRevolverScheduler.waitNanos(LightArrayRevolverScheduler.scala:99)
	at org.apache.pekko.actor.LightArrayRevolverScheduler$$anon$3.nextTick(LightArrayRevolverScheduler.scala:310)
	at org.apache.pekko.actor.LightArrayRevolverScheduler$$anon$3.run(LightArrayRevolverScheduler.scala:280)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"flink-pekko.actor.internal-dispatcher-2" prio=5 Id=35 WAITING on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@6ada992
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@6ada992
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.park(LockSupport.java:341)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.awaitWork(ForkJoinPool.java:1724)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1623)
	at java.base@17.0.7/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

"flink-pekko.actor.internal-dispatcher-3" prio=5 Id=36 TIMED_WAITING on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@6ada992
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@6ada992
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.parkUntil(LockSupport.java:410)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.awaitWork(ForkJoinPool.java:1726)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1623)
	at java.base@17.0.7/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

"flink-pekko.actor.default-dispatcher-4" prio=5 Id=37 RUNNABLE
	at app//org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl.isDynamicIndex(TaskSlotTableImpl.java:371)
	at app//org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl.createSlotReport(TaskSlotTableImpl.java:254)
	at app//org.apache.flink.runtime.taskexecutor.TaskExecutor$ResourceManagerHeartbeatListener.retrievePayload(TaskExecutor.java:2645)
	at app//org.apache.flink.runtime.taskexecutor.TaskExecutor$ResourceManagerHeartbeatListener.retrievePayload(TaskExecutor.java:2597)
	at app//org.apache.flink.runtime.heartbeat.HeartbeatManagerImpl.requestHeartbeat(HeartbeatManagerImpl.java:237)
	at app//org.apache.flink.runtime.taskexecutor.TaskExecutor.heartbeatFromResourceManager(TaskExecutor.java:1001)
	at java.base@17.0.7/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base@17.0.7/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77)
	...

"flink-pekko.actor.supervisor-dispatcher-5" prio=5 Id=38 TIMED_WAITING on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@28d5ed5a
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@28d5ed5a
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:252)
	at java.base@17.0.7/java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.awaitNanos(AbstractQueuedSynchronizer.java:1672)
	at java.base@17.0.7/java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:460)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:1061)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1122)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"flink-metrics-scheduler-1" prio=5 Id=39 TIMED_WAITING
	at java.base@17.0.7/java.lang.Thread.sleep(Native Method)
	at org.apache.pekko.actor.LightArrayRevolverScheduler.waitNanos(LightArrayRevolverScheduler.scala:99)
	at org.apache.pekko.actor.LightArrayRevolverScheduler$$anon$3.nextTick(LightArrayRevolverScheduler.scala:310)
	at org.apache.pekko.actor.LightArrayRevolverScheduler$$anon$3.run(LightArrayRevolverScheduler.scala:280)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"flink-metrics-pekko.actor.internal-dispatcher-2" prio=5 Id=40 TIMED_WAITING on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@5271fdc2
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@5271fdc2
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.parkUntil(LockSupport.java:410)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.awaitWork(ForkJoinPool.java:1726)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1623)
	at java.base@17.0.7/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

"flink-metrics-pekko.actor.internal-dispatcher-3" prio=5 Id=41 WAITING on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@5271fdc2
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@5271fdc2
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.park(LockSupport.java:341)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.awaitWork(ForkJoinPool.java:1724)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1623)
	at java.base@17.0.7/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

"flink-metrics-pekko.actor.internal-dispatcher-4" prio=5 Id=42 WAITING on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@5271fdc2
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on org.apache.pekko.dispatch.ForkJoinExecutorConfigurator$PekkoForkJoinPool@5271fdc2
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.park(LockSupport.java:341)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.awaitWork(ForkJoinPool.java:1724)
	at java.base@17.0.7/java.util.concurrent.ForkJoinPool.runWorker(ForkJoinPool.java:1623)
	at java.base@17.0.7/java.util.concurrent.ForkJoinWorkerThread.run(ForkJoinWorkerThread.java:165)

"flink-metrics-5" prio=1 Id=43 TIMED_WAITING on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@736265
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@736265
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:252)
	at java.base@17.0.7/java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.awaitNanos(AbstractQueuedSynchronizer.java:1672)
	at java.base@17.0.7/java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:460)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:1061)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1122)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"flink-metrics-pekko.actor.supervisor-dispatcher-6" prio=5 Id=44 TIMED_WAITING on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@6bf9aac0
	at java.base@17.0.7/jdk.internal.misc.Unsafe.park(Native Method)
	-  waiting on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@6bf9aac0
	at java.base@17.0.7/java.util.concurrent.locks.LockSupport.parkNanos(LockSupport.java:252)
	at java.base@17.0.7/java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.awaitNanos(AbstractQueuedSynchronizer.java:1672)
	at java.base@17.0.7/java.util.concurrent.LinkedBlockingQueue.poll(LinkedBlockingQueue.java:460)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.getTask(ThreadPoolExecutor.java:1061)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1122)
	at java.base@17.0.7/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base@17.0.7/java.lang.Thread.run(Thread.java:833)

"Timer-0" daemon prio=5 Id=46 TIMED_WAITING on java.util.TaskQueue@6cc135d8
	at java.base@17.0.7/java.lang.Object.wait(Native Method)
	-  waiting on java.util.TaskQueue@6cc135d8
	at java.base@17.0.7/java.util.TimerThread.mainLoop(Timer.java:563)
	at java.base@17.0.7/java.util.TimerThread.run(Timer.java:516)
```