# objstore

对象存储上传与额度核算库。对象内容存本地目录，元数据与账务存 SQLite，
由上层存储服务以库的形式调用，不含接口层。

## 特性

- 对象键为内容的 SHA-256；同账户内同内容只计一次费，跨账户各自全额计费，
  磁盘上只存一份（引用计数管理，最后持有者清除后删文件）。
- 一次性上传与分段上传（5 MiB 分段，乱序/并行/重复传，最后写入为准）。
- 上传按声明大小预占额度，成功后转为计费；账户内去重命中（含回收站恢复）
  时预占全部释放；失败时用量回滚、不留残余。
- 删除进回收站留存 7 天照常计费，可恢复；`cleanup()` 清除到期回收对象并
  释放额度，同时中止 24 小时无新段的分段会话并释放其预占。
- 额度计数（已用/预占）存在账户行上，查询为 O(1)，不随对象数增长变慢。
- 数据流式读写（1 MiB 缓冲），内存占用不随对象大小增长；大上传进行中
  不阻塞同账户其他操作（流式传输在数据库事务之外）。

## 用法

```python
from objstore import ObjectStore

store = ObjectStore("/var/lib/objstore")
store.create_account("team-a")                 # 默认额度 10 GB
key = store.upload("team-a", len(data), stream)

sid = store.create_session("team-a", total_size)
store.put_part(sid, 1, part_stream)            # 段号从 1 开始，5 MiB 一段
store.put_part(sid, 2, part_stream)
key = store.commit_session(sid)                # 幂等，并发提交只计一次费

store.delete("team-a", key)                    # 进回收站，照常计费
store.restore("team-a", key)                   # 恢复
store.cleanup()                                # 由上层定时调用
store.set_quota("team-a", 20 * 1024**3)
usage = store.usage("team-a")                  # {'quota', 'used', 'reserved'}
```

## 测试

```
python3 -m pytest tests/ -q
```
