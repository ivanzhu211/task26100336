# objstore — 对象存储上传与额度核算库

供上层存储服务调用的 Python 库（无接口层）。对象内容存本地目录，
元数据与账务存 SQLite。

## 规则概要

- 对象的键是内容的 SHA-256；同一账户同内容只计一次费，跨账户各自全额计费、
  磁盘只存一份（`blobs` 引用计数归零时删文件）。
- 上传一律先按声明大小**预占**额度（已用+预占超额直接拒绝），成功后预占转计费；
  同账户内容已存在（正常或回收站中）则不加费、预占全释放；上传失败不留残余。
- 分段上传：开会话声明总大小，按 5 MiB 分段（最后一段可更短），段可乱序/
  并行/重复传（同段以最后一次为准）；段不齐或总大小不符时提交失败、会话保留；
  同一会话重复/并发提交只产生一个对象、只计一次费。
- 删除进回收站留存 7 天，期间照常计费、可恢复；`cleanup()` 由上层定时触发，
  彻底清除到期回收站对象并释放额度，同时放弃闲置 24 小时的分段会话
  （释放预占、删掉已传的段）。
- 新账户默认额度 10 GB；额度下调到低于用量时，已有对象与已批准预占不受影响，
  但用量降回额度内之前新上传一律拒绝。

## 用法

```python
from objstore import ObjectStore

store = ObjectStore("meta.db", "data/")          # data/blobs 存内容，data/tmp 存在途数据
store.create_account("team-a")                   # 默认 10 GB
store.set_quota("team-a", 20 * 1024**3)

key = store.put("team-a", size, fileobj)         # 一次性上传，返回内容键

sid = store.begin_upload("team-a", total_size)   # 分段上传
store.upload_part(sid, 1, part_stream)           # 段号从 1 开始
key = store.commit_upload(sid)

store.delete("team-a", key)                      # 进回收站
store.restore("team-a", key)                     # 恢复
store.cleanup()                                  # 定时触发：清回收站 + 放弃闲置会话

usage = store.usage("team-a")                    # O(1)：used / reserved / quota
with store.open_reader("team-a", key) as f:      # 读回内容
    data = f.read()
```

`ObjectStore(...)` 的可选参数：`part_size`（默认 5 MiB）、`max_object_bytes`
（默认 500 MB）、`default_quota_bytes`（默认 10 GB）、`time_fn`（便于测试注入时钟）。

## 并发设计

- 账务与元数据全部在 SQLite 短事务内完成（单连接 + 可重入锁）；
  数据流读写、哈希、拼段都在事务外，且分块（1 MiB）进行——
  500 MB 上传进行中，其他操作不排队，内存占用恒定。
- 已用/预占是随每次账务操作事务性维护的计数器，查用量与对象数量无关。
- 预占以 `reservations` 行的存在为准，释放即删行：不重复释放、不遗漏。
- 每个分段会话一把锁，并发提交只生效一次；会话状态机
  `open → committing → committed / abandoned` 保证提交与过期清理互斥。

## 测试

```bash
python3 -m pytest tests/ -q
```

覆盖：一次性/分段上传、去重、额度拒绝与下调、失败回滚、删除/恢复/清理、
会话放弃，以及验收要求的多账户多线程混合负载对账、
「回收站到期清理 × 同内容上传」「会话提交 × 过期清理」两路并发竞态。
