+++
date = '2024-06-18T17:59:59+08:00'
draft = false
title = 'Kafka源码剖析--副本迁移&扩容'
+++

基于Kafka 2.7.1版本：https://github.com/apache/kafka/tree/2.7.1
阅读过程考虑以下问题点：
- 副本迁移过程中出现宕机或其他问题，是否影响迁移
- 当大量副本同时完成迁移，是否在同一时间进行选举，同时选举是否会发生问题
- 当拉取到副本不存在的offset会如何处理
  kafka提供副本的重定义操作，通常在以下运维场景中用到：
- 副本分配不均匀，导致集群负载倾斜，使用副本重定义进行副本迁移
- 增加&减少副本数量
- broker删除或新增，需要进行副本迁移
  此功能下文统称副本「迁移」

### 1.大致流程
- 1.通过AdminClient发送副本迁移请求
- 2.controller接收请求后，向所有副本（包括原有副本和新增加的副本）broker发送ApiKeys.LEADER_AND_ISR、ApiKeys.UPDATE_METADATA请求，并更新leader代数
- 3.副本broker接收ApiKeys.LEADER_AND_ISR请求后，加入AR副本，开启新副本同步线程
- 4.副本持续同步，向leader拉取数据，同步过程中，在Partition#updateFollowerFetchState方法判断是否可加入isr，若满足加入isr条件，将当前副本加入isr（修改zk节点状态）
- 5.controller接收到zk节点状态变化，开始处理后续的迁移工作：
    - 5.1.若当前leader已不是可用副本，重新选举leader，并向broker发送选举结果
    - 5.2.向broker下线并删除不再需要的副本
- 6.完成

### 2.代码细节
#### 2.1副本迁移开始
##### 创建迁移任务
通过AdminClient#alterPartitionReassignments向controller发送副本迁移请求【推荐，另外还可通过往zk添加节点数据来触发controller副本迁移】
##### controller接收请求
KafkaApi#handle负责处理所有网络请求，controller通过KafkaApi#handleAlterPartitionReassignmentsRequest处理「副本迁移」请求
controller接收到请求后，发布副本迁移&扩容异步事件（事件类型「ApiPartitionReassignment」），进行异步处理
事件发布：KafkaController#alterPartitionReassignments
事件处理：KafkaController#process
![img.png](controller-process.png)
##### controller异步处理逻辑
在KafkaController#processApiPartitionReassignment（图2-1）中开始处理逻辑，该方法中首先对迁移的副本进行一些有效性检查，过滤掉无效的副本broker id。
在对迁移副本做完检查后，调用maybeTriggerPartitionReassignment（图2-2）和onPartitionReassignment（图2-3）方法来触发迁移。
onPartitionReassignment是副本迁移中关键方法，方法的主体分为两部分：
- 开始迁移：包含两个动作，首先更新所有副本（包括当前副本，和新增加或迁移的副本）的leader代数（不修改leader），并向副本发送ApiKeys.LEADER_AND_ISR和ApiKeys.UPDATE_METADATA请求，将新副本加入AR列表，其次将新加入AR列表的副本状态调整设为NewReplica（发送ApiKeys.LEADER_AND_ISR请求），自此，新副本接收ApiKeys.LEADER_AND_ISR后，开启副本同步线程（2.2中详细介绍）
- 迁移结束后的动作：在副本同步完成，并申请加入到了isr列表后，会触发迁移后的动作（2.3中详细介绍），主要包含以下动作：
    - 1.将新的副本上线，并首先将内存中旧的副本集换为新的副本集
    - 2.判断leader所在副本是否还是可用副本，非可用则进行新的副本选举
    - 3.停止并下线旧的副本
    - 4.通知broker更新元数据
      ![img_1.png](img_1.png)![img_2.png](img_2.png)![img_3.png](img_3.png)
#### 2.2新副本加入isr逻辑
      副本同步&加入isr逻辑涉及的几个类：
      AbstractFetcherManager：同步线程管理的抽象类
      ReplicaFetcherManager：同步线程管理，由此创建同步线程
      ReplicaAlterLogDirsManager：本地拉取线程管理
      AbstractFetcherThread：同步线程的抽象类，包含主体逻辑，抽象了部分方法
      ReplicaFetcherThread：同步线程的实现类
      ReplicaAlterLogDirsThread：本地拉取线程
##### 开启同步线程
在2.1中涉及到新副本接收到ApiKeys.LEADER_AND_ISR请求后，开启同步线程。
首先在副本接收到ApiKeys.LEADER_AND_ISR请求后，在KafkaApis#handleLeaderAndIsrRequest方法中处理请求，并在方法中调用了ReplicaManager#becomeLeaderOrFollower执行具体逻辑，在becomeLeaderOrFollower逻辑中，makeFollowers->AbstractFetcherManager#addFetcherForPartitions开始了副本同步线程。
![img_4.png](img_4.png)
![img_5.png](img_5.png)
##### 副本同步
在第一步创建同步线程中具体的线程类是ReplicaFetcherThread（继承自AbstractFetcherThread）。线程start后，循环执行AbstractFetcherThread#doWork方法。doWork往下追踪：doWork->maybeFetch->processFetchRequest->fetchFromLeader。
- fetchFromLeader从leader中拉取消息
- processPartitionData将消息写入日志段中，并更新高水位
  ![img_6.png](img_6.png)
  ![img_7.png](img_7.png)
##### 加入isr
在第1步的becomeLeaderOrFollower逻辑中，除了创建ReplicaFetcherThread线程，另外通过ReplicaAlterLogDirsManager创建了ReplicaAlterLogDirsThread线程，这个线程启动后会开始持续拉取本地的消息，并判断是否需要加入isr。
加入isr的逻辑链（对应下图2-8～2-12）：
ReplicaAlterLogDirsThread#fetchFromLeader->ReplicaManager#fetchMessages->ReplicaManager#updateFollowerFetchState->Partition#updateFollowerFetchState->Partition#maybeExpandIsr
![img_8.png](img_8.png)![img_9.png](img_9.png)![img_10.png](img_10.png)![img_11.png](img_11.png)![img_12.png](img_12.png)
#### 2.3 迁移后的选举逻辑
##### zk节点监听注册
在副本加入isr后，会讲分区状态写入zk，当controller接收到zk节点信息变更后，执行迁移的后续动作。
首先controller在切换或启动时，会注册zk节点监听。当监听到变更时，会发布IsrChangeNotificationHandler事件，通过异步的方式处理后续逻辑
![img_13.png](img_13.png)
![img_14.png](img_14.png)
##### isr变更，leader副本选举
前文介绍到这部分动作包括了上线新副本、选举新leader、删除和下限旧副本，在这我们探讨一下选举的逻辑。
首先在controller接收到IsrChangeNotificationHandler事件后，经过processIsrChangeNotification->maybeCompleteReassignment->onPartitionReassignment进入到下图2-14逻辑，该逻辑为副本迁移的重要方法。
其中会调用到moveReassignedPartitionLeaderIfRequired，该方法负责处理迁移后的选举相关逻辑，方法逻辑中主要包含三个分支：
- 若当前leader已不再副本列表，执行选举
- 若当前leader仍在副本列表且可用，更新leader的代数即可
- 若当前leader仍在副本列表但不可用，执行选举
  ![img_15.png](img_15.png)![img_16.png](img_16.png)
### Q&A
Q：宕机是否影响迁移进度
A：宕机重启后，自动恢复拉取线程，若宕机时间过长，可能导致读取冷数据影响leader性能
Q：当大量副本同时完成迁移，并触发选举，是否会存在性能问题
A：不存在controller性能问题，可能达到zk写瓶颈：
- controller通过zk监听的方式触发，因此zk的写瓶颈大于controller处理瓶颈，且controller是异步单线程串行
- 当controller触发了大量的选举后，同时会有大量的写zk动作（同步）+网络请求（异步）
  Q：当拉取副本不存在的offset
  A：当前开源版本拉取到不存在的offset时，抛出OffsetOutOfRangeException异常（代码位置：Partition.scala#readRecords，1119行）。后续增加了平滑扩容功能，需要考虑修改逻辑来支持fetch转发到能拉取到消息的副本
