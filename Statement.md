# 声明

此项目目前仅供学习使用，内容来源于Raft论文，框架来源于Mit 6.5840 Lab2。 实用的RPC接口和持久化到非易失性存储的方法还未实现。

## 一些功能实现方法的说明(未在论文中出现的)

超时设置由Go计时器实现，启用一个单独的线程等待计时器的超时。

对Raft实例状态量的访问控制采取比较简单保守的方法，在每一个可能并发的场合都加上锁

在每个请求投票RPC的回复处理中，检查已经收到的票数，决定是否要成为Leader

当心跳计时器超时，Leader将会向所有服务器发送AppendEntries RPC，Leader检查nextIndex和自己的日志，决定是否要在AppendEntries RPC中带上新的日志，或者是发送快照

Leader在每次AppendEntries RPC回复时，检查是否要增加自己的commitIndex。具体方法为：排序找出matchIndex的中位数，若中位数大于当前的commitIndex，就把commitIndex更新为这个中位数

AppendEntries RPC回复失败后会立即重新发送

对于Leader发送来的日志，Follower将它们与自己的一一对比，若相同则不做处理；若不相同，截去矛盾的和之后的所有日志，添加来自Leader的日志
