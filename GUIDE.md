# relyt-ingest 使用指南

面向以 Rust 程序（典型为 Kafka 消费者）向 Relyt heap 表实时写入数据的客户。
本文覆盖：接入方式、三种部署拓扑示例、影响数据可见延迟的关键参数，以及无需
关心的内部参数。API 细节以 `cargo doc`（`lib.rs` rustdoc）为准。

## 工作原理（一句话）

SDK 在客户端把 Arrow 批攒成 CSV 文件写到 OSS/S3 staging，然后通知 Relyt
在服务端并行装载（有主键的表走 upsert）——**行数据不经过 master**。
Relyt 服务端保证同一 writer 的文件严格按序、恰好一次地装载：文件重复通知、
进程崩溃重启、消息重投都不会造成丢数或重复。

## 前置条件

- Relyt 集群版本 **3.55.0 及以上**（SDK 摄入所需的服务端能力自该版本起提供）；
- **staging 桶：默认无需你准备**。SDK 在 `Client::connect` 时向 Relyt 服务端
  索取桶地址与凭证，所以你的配置和代码仓库里不会出现任何密钥。只有当你要求
  数据中转必须落在自己账号的桶里时，才改用自备桶模式，此时需要一个 OSS 或 S3
  桶及其 AK/SK，凭证要对 `<prefix>/staging/*` 与 `<prefix>/_meta/*` 有读、写、
  删权限。两种模式的差别见下文「staging 桶归属」；
- 桶生命周期规则：只挂在 `<prefix>/staging/` 上，**绝不能**挂到
  `<prefix>/_meta/`（那里存放断点续传状态）。默认模式下由 Relyt 配置，自备桶
  模式下由你配置；
- 目标表为 heap 表；upsert 模式要求有 PRIMARY KEY，insert-only 模式不需要；
- **Rust 与 Arrow 版本**：`append` 直接收 Arrow `RecordBatch`，而不同大版本的 arrow
  类型互不兼容，所以 SDK 锁定 `arrow-array` / `arrow-schema` **58.x**（对应
  `deltalake` 0.32.x），你的工程需用同一大版本，升级前请与我们同步。SDK 的最低
  Rust 版本是 **1.85**（CI 每轮以该版本编译一次来保证），不设上限；实际下限通常由
  你工程里更高的依赖决定，例如 `deltalake` 0.32.x 要求 **1.91.1**，cargo 对此是硬报错；
- 数据库账号：使用 Relyt 管理员提供的摄入账号即可（最小权限，无需
  superuser）。

## 依赖方式

```toml
[dependencies]
relyt-ingest = "0.1"
arrow-array  = "58"   # append 直接收 RecordBatch，arrow 大版本必须与 SDK 一致
```

crate 发布在 crates.io，API 文档在 docs.rs。arrow 与 Rust 版本约束见上文「前置条件」。

## 接入信息清单（谁提供、落到哪个字段）

| 信息 | 提供方 | SDK 中的位置 |
|---|---|---|
| Relyt 控制连接：host / port / dbname + **摄入账号**用户名密码（最小权限账号，无需 superuser） | Relyt 管理员 | `ClientConfig::new(control_dsn)` 的唯一参数，tokio-postgres 连接串格式 `host=... port=... user=... password=... dbname=...` |
| staging 桶 | **默认由 Relyt 提供**，SDK 在 connect 时自动索取，你无需填写任何内容；仅自备桶模式下由你提供 endpoint、bucket、prefix、AK/SK（endpoint 为 AWS、腾讯 COS、金山 KS3、UCloud、火山 TOS 的标准域名时 region 自动识别，MinIO/R2 等需显式填；**目前 e2e 只覆盖阿里云 OSS 与 AWS S3，其余厂商仅按域名识别并以 S3 兼容方式签名，尚无用例覆盖**） | 默认模式：无；自备桶模式：`ClientConfig::with_customer_staging(StagingConfig { .. }, dsn)` |
| 桶生命周期规则 | 桶所有方在云控制台配置：挂 `<prefix>/staging/`，**不挂** `_meta/` | 无（SDK 不感知） |
| Relyt 实例标识（instance id） | 一般**不需要**——SDK 自动向服务端获取；仅实例未配置时由 Relyt 管理员提供 | `ClientConfig::cluster_id`（`Option<String>`） |
| 目标表 | 客户自行建表（heap 表；upsert 需 PRIMARY KEY；列类型见「支持的列类型」） | `open_table("schema.table", writer_id)` |
| 版本前提 | Relyt 集群 ≥ 3.55.0；客户工程 arrow 58 / Rust ≥ 1.85（本 SDK 的下限，不设上限；`deltalake` 0.32.x 自身要求 ≥ 1.91.1，通常由它决定实际下限） | 编译期 |

**SDK 不读取任何环境变量**（仅租约身份读 `HOSTNAME` 作机器名）——上述信息全部
通过 `ClientConfig` 结构体在代码里传入。

### staging 桶归属

| | 默认（Relyt 托管） | 自备桶 |
|---|---|---|
| 怎么写 | `ClientConfig::new(dsn)` | `ClientConfig::with_customer_staging(staging, dsn)` |
| 桶与凭证 | connect 时向服务端索取，**你的配置与代码仓库里没有任何密钥** | 你提供并自行保管 |
| 生命周期规则 | Relyt 配置 | 你配置；队列积压时 staging 增长记在你的账单上 |
| 区域 | Relyt 保证与实例同区 | 你需保证与 Relyt 实例同云同区域，服务端要直连读取 |
| 安全姿态 | 你的人与配置系统不接触密钥 | **你的密钥必然到达 Relyt 服务端**：Relyt master 装载时必须能读你写入的对象，凭证会随装载任务传给它并落入作业记录。这一点与默认模式不同，请知悉后再选择 |

两者是同一个字段 `staging` 的两个取值（`Staging::Relyt` 与 `Staging::Customer(..)`），
"填了自己的桶却忘了切换归属"这种中间状态在类型上就不存在。

```rust
// 默认：无需任何凭证
let cfg = ClientConfig::new(env("RELYT_DSN"));

// 自备桶
let cfg = ClientConfig::with_customer_staging(
    StagingConfig {
        service: StagingService::Oss,
        endpoint: env("RELYT_STAGING_ENDPOINT"),
        bucket: env("RELYT_STAGING_BUCKET"),
        prefix: env("RELYT_STAGING_PREFIX"),
        access_key_id: env("RELYT_STAGING_AK"),
        secret_access_key: env("RELYT_STAGING_SK"),
        region: std::env::var("RELYT_STAGING_REGION").ok(),
    },
    env("RELYT_DSN"),
);
// Client::connect 会校验：归属与配置是否一致、凭证为空/含 `,`/`"`、
// S3 无法确定 region、参数越界——都在此报错，不会拖到第一次上传
```

默认模式下 connect 可能返回的三类错误：实例尚未开通摄入 staging（联系 Relyt
管理员），摄入账号无权调用该接口（同上），以及服务端版本过旧（升级实例，或改用
自备桶模式）。

## 凭证管理与轮转

**默认模式下你不持有 staging 凭证**，只剩 DSN 要保管：不要写死在源码或配置仓库，
用 K8s Secret、0400 文件或密钥管理服务注入。staging 密钥轮转由 Relyt 侧完成，
**你无需改配置或重启**：SDK 每 5 分钟（`staging_refresh_interval`）重新获取一次，
上传被拒（403）时立即刷新并重试；Relyt 侧保证旧密钥至少再有效 24 小时，覆盖刷新
周期与在途装载任务的重试期。

自备桶模式下由你负责：

- AK/SK 同样不要写死在源码或配置仓库里；
- 按**最小权限**签发（仅限该 staging 前缀），不在多个系统间复用；
- **用静态 AK/SK 而非 STS 临时凭证**：凭证随每个装载任务交给服务端读取 staging
  文件，任务可能数小时内重试，临时凭证过期会让在途任务失败；
- **轮转步骤**：签发新密钥 → 切换配置并优雅重启（`close()` 后新进程 `open_table`，
  零停写）→ **旧密钥保留至少 24 小时**再吊销。

两种模式下 SDK 的日志与 `Debug` 输出都对凭证脱敏。

## 快速开始（单表单 partition）

一个 Kafka partition 对应一个 writer。完整可编译示例：
[`examples/kafka_partition_writer.rs`](examples/kafka_partition_writer.rs)。

```rust
use relyt_ingest::{Client, ClientConfig, StreamMode};

// staging 默认由 Relyt 托管：connect 时自动取得桶与凭证，这里不需要任何密钥。
// 要用自己的桶，改为 ClientConfig::with_customer_staging(staging, dsn)。
let mut cfg = ClientConfig::new("host=... port=5432 user=ingest dbname=prod");
cfg.stream_mode = StreamMode::Upsert;                // 默认值；无主键表用 InsertOnly

let client = Client::connect(cfg).await?;
let (writer, plan) = client.open_table("public.orders", "orders-p0").await?;

// 1. 把 Kafka consumer seek 到 plan.kafka_resume_offset（下一条要消费的
//    offset；None 表示无历史记录，用你自己的 checkpoint）。
// 2. 消费循环：每批 append，不需要每批 flush——文件由攒批参数自动切割。
writer.append(batch, start_offset, end_offset).await?;

// 3. 提交 Kafka offset 的门槛：writer.staged_offset() >= 该批的 end_offset。
//    （staged_offset 是"最后一条已持久化"，含；建议周期性提交。）
// 4. 进程退出（含滚动升级的 SIGTERM）时调用 close()：排空尾巴并立即释放
//    writer 租约，接替进程零等待启动：
writer.close().await?;
```

**同一个 writer 的 `append` 必须串行调用**（一个任务按 offset 递增顺序调用，通常就是
该 partition 的消费循环）。SDK 要求同一 writer 内的 offset 严格向前，并发调用会让两段
数据互相穿插，表现为 `append` 返回 offset 回退的 `Config` 错误。多个 partition 请各用
各的 writer，而不是多任务共用一个。

**writer_id 命名**：唯一标识一条流，重启前后必须一致；建议 `"<topic>-p<分区号>"`
（如 `"orders-p0"`），带上 topic 避免两个 topic 写同一张表时撞名。字符集
`[A-Za-z0-9._-]`，不以 `_` 开头，最长 64 字节。**同一个 writer_id 同时只能有
一个进程在跑**——SDK 用 staging 上的租约强制这一点：第二个进程 `open_table`
会报 `WriterLocked`；原进程崩溃后约 3 分钟租约过期，新进程自动接管。

## 三种部署拓扑

### 1. 单表单 partition

即快速开始。一个进程、一个 `Client`、一个 writer。

### 2. 单表多 partition

**每个 partition 一个 writer**（通常也是一个进程/容器；同进程内多个 tokio
任务在语义上等价——每个 writer 有独立的连接、通知队列与租约身份）。

> **使用契约：同一个主键的所有消息必须始终落在同一个 Kafka partition**（即
> producer 按主键/业务键做 key 分区——Kafka 的默认做法）。各 writer 之间不保序，
> 只在 writer 内部严格有序；若同一主键的两次更新被打到两个 partition，会经两个
> writer 并行装载，先后顺序不确定，表里的终值成为静默的竞争结果——SDK 与服务端
> 都无法检测到这种情况。满足契约时各 writer 天然写不相交的 key，同表并发安全。

完整示例：
[`examples/single_table_multi_partition.rs`](examples/single_table_multi_partition.rs)。

```rust
// 每个 partition 各自执行（独立进程或任务）：
let client = Client::connect(cfg.clone()).await?;
let (writer, plan) = client
    .open_table("public.orders", &format!("orders-p{partition}"))
    .await?;
// ... 各自消费自己 partition、各自按 plan 恢复位点
```

扩分区 / consumer rebalance 后，新进程用同一 writer_id 重新 `open_table`
即可从断点继续——恢复位点、补录、去重全部自动完成。

### 3. 多表多 partition

每个 `(表, partition)` 一对 writer；不同表的流互不影响（一张表卡住只会停
自己的通知队列）。一个进程可以同时持有多个表的 writer，也可以按表拆进程。
完整示例（同时演示两种模式）：
[`examples/multi_table_multi_partition.rs`](examples/multi_table_multi_partition.rs)。

```rust
tokio::join!(
    run_stream("public.orders", "orders", 0, StreamMode::Upsert),
    run_stream("public.orders", "orders", 1, StreamMode::Upsert),
    run_stream("public.clicks", "clicks", 0, StreamMode::InsertOnly), // 无主键表
    run_stream("public.clicks", "clicks", 1, StreamMode::InsertOnly),
);
```

### 容量估算（多 writer）

每个 writer 独立连接、独立缓冲是有意的故障隔离设计，代价是三项资源随
W（表数 × 分区数，同进程内）线性增长，部署前请估算：

| 资源 | 公式 | 说明 |
|---|---|---|
| Relyt 常驻连接数 | **1 + 2W**：1 条控制连接，外加每 writer 2 条（通知队列 1 条、滞后采样 1 条）。另有短命连接：每 writer 每小时 1 条做 GC；托管模式下每**进程**每 5 分钟 1 条刷新凭证 | 需对照实例的连接数配额 |
| 内存上界 | 单 writer：**(6 + `rotation_queue_depth`) × `rotate_size_bytes`**；多 writer：**W × (6 + `rotation_queue_depth`) × `rotate_size_bytes`** | 默认值代入：(6 + 3) × 64 MB = **576 MB / writer**。6 = 1 份在填的缓冲 + 切文件流水线三段及段间各约 1 份（偏保守）；队列满时 `append` 阻塞，所以是硬上界，实际驻留通常远低于它 |
| CPU | 单 writer **不超过 2 核**；多 writer 线性叠加 | 只有渲染与压缩两段吃 CPU，各占一条线程且每段一次只处理一个文件，所以单 writer 有硬上界 2 核，实际持续占用在 1~2 核之间、随吞吐变化。**压缩是其中的大头**，关闭压缩（`staging_compression = Plain`）可显著降低，代价是上传字节与对象存储占用成倍增加 |
| 后台任务 | 7W 个 tokio task（ticker / GC / 租约心跳 / lag 采样 / render / gzip / put）+ W 条 notify 循环 | 可忽略 |
| tokio blocking 线程 | 每 writer 峰值 **2 个**（渲染、压缩各一，仅在真正干活时占用）；tokio 默认上限 512 | W > 256 且多数 writer 同时满负荷时需调大运行时的 `max_blocking_threads`。超出只会排队、不报错，表现为吞吐下降与 `lag_seconds` 上升。这些线程的栈不计入上面的内存公式 |

示例：10 表 × 32 分区 = 320 writer → 约 641 条常驻连接、内存上界 320 × 576 MB ≈ 180 GB；
把 `rotate_size_bytes` 调到 16 MB 则约 45 GB。W 较大时就这样调小它（或依赖 15 秒时间
阈值切文件），并与 Relyt 管理员确认连接数配额。

**单 writer 跑在容器里给多少规格**：写入侧本身的内存按上面的公式算（默认 576 MB），
CPU 上界 2 核。给容器定规格时注意两点：

- 上面的 CPU 只是**写入通路**的开销，不含应用自己消费消息、反序列化成 RecordBatch、
  做业务转换的部分。数据通路轻的给 2 核即可；要解码转换的建议 4 核，否则两者抢同样的核。
- 把 `rotate_size_bytes` 调小可以压低内存，但**不会降低 CPU**——压缩与渲染的成本按字节算。
  需要更高吞吐时加 writer（CPU 随之线性增加），而不是给单个 writer 加核。

## 支持的列类型

`open_table` 时按目标表的列类型逐列校验，白名单外的列直接报
`UnsupportedType`（不会开始写）。`append` 传入的 RecordBatch 每列必须是右侧
对应的 arrow 类型（用 `writer.schema()` 构造即可自动对齐）：

| Relyt 列类型 | 批中的 arrow 类型 | 说明 |
|---|---|---|
| `boolean` | `Boolean` | 渲染为 `t`/`f` |
| `smallint` / `integer` / `bigint` | `Int16` / `Int32` / `Int64` | |
| `real` / `double precision` | `Float32` / `Float64` | 普通值精确；`NaN`/`±Infinity` 目前不保证与服务端约定一致，避免写入 |
| `numeric(p,s)` / `decimal(p,s)`，p ≤ 38 | `Decimal128(p, s)` | **精度与标度必须与表定义完全一致**；不带精度的裸 `numeric` 不支持 |
| `text` / `varchar` / `varchar(n)` / `char(n)` | `Utf8` | 任意 Unicode（含中文、emoji、换行、引号、分隔符本身）原样写入；仅 `\0` 被剥离 |
| `date` | `Date32` | |
| `timestamp` (without time zone) | `Timestamp(Microsecond, None)` | |
| `timestamp with time zone` | `Timestamp(Microsecond, Some("UTC"))` | 值为 UTC 瞬时；SDK 带 `+00` 偏移写入，服务端不会按会话时区误移 |
| `bytea` | `Binary` | 以 `\x` 开头的十六进制形式写入 |

**不支持**（需要请与我们同步）：`json`/`jsonb`、`uuid`、数组（含 `array(row(...))`
这类嵌套结构）、`interval`、`time`、`inet` 等；`numeric` 精度 > 38。

**主键列的类型限制**（仅 upsert 模式）：上表中除**浮点（`real` / `double precision`）**
外的类型都可以做主键。浮点不行的原因是 `NaN` 与 `±0.0` 的相等判定在客户端与服务端不
一致，同一份数据可能去重出不同结果；这类表在 `open_table` 时即报 `UnsupportedType` 并
点名该列。insert-only 模式不做去重，因此没有这条限制，浮点主键的表照常可写。

## 客户需要关心的参数

### 攒批参数（直接决定数据多久可见）

数据从 `append` 到可查询要经历：**攒批 → 文件落 staging → 服务端装载**。
文件在两个条件先到者触发时切割上传：

| 参数 | 默认值 | 合法范围 | 含义 |
|---|---|---|---|
| `rotate_size_bytes` | **64 MB** | (0, 512 MiB] | 缓冲区攒到该大小（按渲染后 CSV 字节估算）即切一个文件；上限对应"整文件在内存中渲染、单次上传"的实现方式 |
| `rotate_interval_max` | **15 秒** | [1 秒, 6 小时] | 最老一条缓冲数据满该时长即切文件（后台自动，兜底低流量） |
| `rotation_queue_depth` | **3** | [1, 16] | 已切出、等待后台流水线（渲染 → gzip → 上传）接手的文件最多攒几个；攒满后触发切文件的那次 `append` 阻塞等待（背压）。1 格让流水线不停，其余吸收突发；每格一份 `rotate_size_bytes` 的内存（见「容量估算」） |

所有参数在 `Client::connect` 时做范围校验，单位写错（比如把字节当成了 MB）
会立即报错并提示合法范围，而不是运行后行为异常。

**可见延迟 ≈ min(攒满 64MB 的时间, 15s) + 服务端装载耗时（通常秒级）**。
也就是说默认配置下，低流量流的数据最迟约 15 秒 + 装载时间可见；高流量流按
64MB 一个文件的节奏持续可见。调优方向：

- 要更低延迟：调小 `rotate_interval_max`（例如 5s）。代价是文件更小更多，
  服务端装载任务数增加；不建议低于 2~3 秒。
- 要更高吞吐/更少文件：保持或调大 `rotate_size_bytes`。64MB 是服务端装载
  效率与延迟的平衡点，一般不需要动。
- 每次 `append` 的批大小（多少行调用一次）不影响正确性，只影响调用开销；
  常见做法是按 Kafka poll 批直接透传。
- 需要立即可见（如退出前、测试中）：显式 `writer.flush()`。

### 其它需要设置/了解的参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `staging` | `Staging::Relyt` | 桶来源。默认由服务端提供桶与凭证；`Staging::Customer(StagingConfig { endpoint/bucket/prefix/AK/SK/service/region })` 表示自备桶，`region` 仅 MinIO/R2 等无法从域名识别的端点需要显式填（AWS/腾讯 COS/金山 KS3/UCloud/火山 TOS 自动识别；e2e 只覆盖 OSS 与 AWS S3）。见「staging 桶归属」 |
| `control_dsn` | 必填 | Relyt 控制连接串（仅元数据与通知，不走行数据） |
| `stream_mode` | `Upsert` | 有主键表用 `Upsert`（同 key 终值=最后一次写入）；无主键表用 `InsertOnly`（重复行原样保留）。**同一张表的所有 writer 必须同模式**；切换模式前需停写排空 |
| `staging_compression` | `Gzip` | staged CSV 文件 gzip 压缩后上传（对象名 `.csv.gz`）；Relyt 服务端按文件内容自动识别并解压，**无需任何服务端配置**。设为 `Plain` 得到可直接下载阅读的明文 `.csv`（排障时有用）。攒批阈值始终按压缩前的 CSV 大小判定。取舍：压缩比取决于数据形态（宽字符串表通常是几倍），省下的是上传带宽与 staging 存储；代价主要是**客户端 CPU**——压缩是写入通路里最重的一段。Relyt 侧解压的开销很小，**所以这是客户端 CPU 与带宽/存储之间的取舍，与服务端关系不大** |
| `cluster_id` | `None` | Relyt 实例标识（instance id——一个 Relyt 实例即一个 DWSU，对应一个 id），用作 staging 路径的命名空间，隔离多个实例共用一个桶的场景。一般不用设：SDK 自动向服务端获取。仅当实例未配置该标识（connect 报错提示时）才需显式指定 |

### 错误处理（消费循环里必须处理的几类）

| 错误 | 含义 | 正确动作 |
|---|---|---|
| `StagingStalled`（`append`/`flush`/`close` 返回） | staging 持续不可达：流水线队头文件连续失败 `staging_error_after_attempts` 次。**已切出的行仍在内存里**，流水线继续按 1s→30s 退避重试同一份字节。队列满时 `append` 先阻塞（背压），达到阈值后也返回它——**这一批数据未被接收** | `append` 收到：稍后重试同一批。`flush` 收到：**稍后继续 `flush`**，不要重开 writer。期间都不要提交对应的 Kafka offset |
| `RotationFailed`（`append`/`flush`/`close` 返回） | 已切出的文件在渲染/压缩/上传的某一步反复失败，且重试不会好（错误信息带出具体原因）。与 `StagingStalled` 的区别就是"等下去没用" | **不要继续 `flush` 等待**，也不要提交对应的 Kafka offset；按错误信息修掉根因（多为数据或表定义问题）后重启，由恢复握手重放 |
| `StagingOrderViolation`（`append`/`flush`/`close` 返回） | SDK 内部不变量被破坏：文件没有按 seq 顺序到达上传段。错误信息里带**缺失的 seq 与 Kafka offset 区间** | **停止该流的消费、不要提交 Kafka offset**、上报；回滚或升级 SDK 后按下一节人工回拨位点再启动。**重启不会自愈** |
| `close()` 返回上面任一错误 | `close(self)` 按值消费 writer：无论返回什么，writer 都已结束，缓冲与在途文件随之丢弃 | **不要提交对应的 Kafka offset**，重启后由恢复握手从 `RecoveryPlan::kafka_resume_offset` 续读即可。想在 writer 还可用时看到并等待流水线恢复，就先调 `flush()` |
| `append` 返回 `Config`（offset 回退） | 这一批的 `start_offset` ≤ 本 writer 已 append 过的最大 `end_offset`：消费位点被回拨（rebalance、seek）到已 append 的区间。**数据未入缓冲**，writer 仍可用 | 不要对同一 writer 重放：重开该表（`open_table`）并从 `RecoveryPlan::kafka_resume_offset` 续读 |
| `WriterFenced` / `SerialContractViolation` | 本 writer 已被另一进程接管，或 writer 身份冲突 | 停止本进程该流的消费，排查是否有重复部署 |
| `open_table` 返回 `WriterLocked` | 该 writer_id 已有活跃进程 | 不要强行启动；确认旧进程已死可等约 3 分钟租约过期自动接管 |
| 其余错误（网络、控制连接） | SDK 内部已做重试，透出的是可重试故障 | 退避后重开 writer，断点自动恢复 |

### 什么时候需要人工回拨 Kafka 位点

SDK 的自动恢复只能表达"从已落地的最后一个文件之后继续"（续读位点 = 全部
staged 文件 end 的最大值 + 1），表达不了"前沿后面还有一个洞"。所以：

| 情形 | 要不要回拨 |
|---|---|
| `StagingOrderViolation` | **要**。缺口在前沿之后，恢复握手会跳过它 |
| `StagingStalled` 或 `RotationFailed` 后重启 | 不要。未落地的行从未进过 staging，续读位点正好落在它们的起点 |
| writer 被抢占（`WriterFenced`），由接任者继续 | 不要。已落地未通知的对象由接任者的 LIST 补录 |
| 进程崩溃重启 | 不要。恢复握手覆盖 |

**怎么回拨**：只动**客户侧 Kafka 消费位点**——消费组 `reset-offsets` 到错误信息给出
的区间起点，或在应用内 `seek` 后再启动。**服务端不需要也不要做任何操作**：回拨后
重放的数据由新的 writer 会话提交，服务端照常装载。

**模式差异**：`Upsert` 下重放安全（同主键末值覆盖）；`InsertOnly` 下重放会把
缺口之后那段已入库的数据再插一遍，需要业务侧按幂等键去重，或先评估重复影响面。

## 无需设置的参数（保持默认即可）

以下参数与服务端装载任务、后台维护相关，默认值已按生产经验设定，**客户不需
要也不建议修改**；仅列出便于理解行为：

| 参数 | 默认值 | 作用（了解即可） |
|---|---|---|
| `retry_max` | 15 | 单个装载任务在服务端的重试预算（约 5 小时后转入人工处置状态，由 Relyt 运维按 SOP 处理坏文件）；改成无限重试会让坏文件永远停不下来 |
| `gc_interval` / `gc_retain_days` / `gc_retain_min_files` | 1h / 7 天 / 5 万 | staging 上已消费文件的自动清理节奏与保留底线 |
| `lock_heartbeat_interval` / `lock_lease_timeout` | 30s / 180s | writer 租约的心跳与接管时限（就是上文"约 3 分钟自动接管"的来源），详见下方说明 |
| `csv.delimiter` | `,` | CSV 列分隔符，与服务端约定一致，无需修改 |
| `staging_refresh_interval` | 5min | 托管模式下重新获取 staging 凭证的周期（上传 403 时另会立即刷新）；自备桶模式忽略 |
| `staging_error_after_attempts` | 3 | 流水线队头文件连续失败到这个次数后，`flush()`/`close()` 不再等待而是返回 `StagingStalled`（在第 3 次失败上报时返回，此前退避 1 s + 2 s ≈ 3 s，不含每次尝试本身的耗时）；流水线本身继续按 1s→30s 退避重试同一份字节，不丢行，之后再调 `flush()` 会继续等。收到该错误时不要提交对应的 Kafka offset |

**关于租约两参数**：它们只服务于"同一 writer_id 同时只有一个进程"的保护，
**不在数据路径上，不影响数据可见延迟**——心跳是独立后台任务，append/flush/
文件切割都不等待它。接管等待时间按退出方式分三档：

| 退出方式 | 接替进程等待 |
|---|---|
| 调用了 `writer.close()`（推荐：接到 SIGTERM 时调用，滚动升级即此场景） | **零等待** |
| 进程被 kill -9 / 崩溃，接替进程在**同一台机器/容器**重启 | **零等待**（SDK 检测到锁持有者的进程已不存在，立即接管） |
| 进程被 kill -9 / 断电，接替进程在**另一台机器**启动 | 最多 `lock_lease_timeout` = 3 分钟（无法证明前任已死，只能等租约过期） |

期间仅该 partition 的摄入暂停，**已写入的数据照常可见**。只有第三档对时间敏感时
才考虑调小租约（如 10s/30s；下限为心跳 ≥1s、租约 ≥3×心跳），代价是长 GC 停顿或
网络抖动超过租约时会被误判死亡而让出流（数据仍安全，进程需重开 writer）。

## 日志

SDK 用 `tracing` 输出结构化日志（接任意 tracing subscriber 即可采集），关键
事件均为一行式：**每个文件落盘**（库/表/writer/对象路径/start-end offset/
行数/字节/耗时）、**每次服务端确认**（往返耗时）、**启动恢复摘要**（续读
位点/补录数/水位）、**租约事件**（获取/接管/释放/被抢占）、GC 轮摘要、
~5 分钟一行的状态心跳。正常路径 INFO、可自愈异常 WARN、停流 ERROR；日志
永不包含凭证。

## 运维须知

- **装载失败文件**（典型成因：某批数据类型不合法）：该 writer 的装载会在
  重试耗尽后停在失败态，**该流的后续文件排队等待，其它表/分区不受影响**。
  联系 Relyt 管理员按 SOP 跳过或修复；SDK 侧 `fatal_error()` 可探测此状态。
- **DROP 后重建同名表**：新表是全新身份，旧 staging 文件成为孤儿，由
  `staging/` 生命周期规则回收，无需手工处理；重建后 writer 从头开始。
- **表重命名**（`ALTER TABLE ... RENAME`）：无影响，断点照常。

## 消费滞后监控与告警建议

数据从 Kafka 到 Relyt 可查询要过三段：**消费（Kafka→SDK）→ 攒批上传
（SDK→staging）→ 服务端装载**。建议按段设三层信号，任何一层劣化都能第一
时间定位到段：

| 信号 | 采集方式 | 建议阈值 | 含义 |
|---|---|---|---|
| Kafka consumer lag | Kafka 侧标准监控（consumer group lag） | 按业务容忍度 | 消费跟不上生产：SDK 进程算力/网络不足，或进程挂了 |
| 持久化滞后 = consumer 当前 offset − `writer.staged_offset()` | 应用内周期采集（如每 30s 导出到监控系统） | 折算时间 > 3 × `rotate_interval_max`（默认即 ~45 秒）持续 2 个采集周期 | 攒批或上传不畅：staging 网络/凭证问题，或单批过大 |
| **装载滞后 `lag_seconds`**（SDK 内置）= 最老未装载文件的年龄 | `writer.lag()`（后台每 30s 通过一条常驻连接采样一次服务端水位，与本进程已切出、尚未被水位覆盖的文件清单相减；文件在切出的那一刻即计入——包括尚未上传完成、正在重试上传的文件，所以 staging 不可达时该值与 `buffered_age_seconds` 会一起增长；进程重启后由恢复阶段的目录清单补齐）导出到监控系统；SDK 同时每 ~5 分钟打一行状态心跳日志 | > 120 秒持续 2 个采样 | 服务端装载不畅（含装载失败文件卡队头——此时该值持续增长） |
| 端到端可见延迟 = now − 表内最新事件时间 | 若表有事件时间列，查询侧探针 `SELECT max(event_time)`（低频，如每分钟） | > 5 × (`rotate_interval_max` + 1 分钟) | 全链路健康的最终裁决，覆盖服务端装载段 |

**必须立即告警的两个状态**（不是滞后，是流已停止，需人工介入）：

- `writer.fatal_error()` 返回 `Some(...)`，或 `append`/`flush` 报
  `WriterFenced` / `SerialContractViolation`：该流已停写。建议应用内每次
  append 失败即上报，并在监控面板暴露 `fatal_error()` 状态；
- 同一批数据反复装载失败（典型是数据类型不合法导致的装载失败文件）：表现为持久化
  滞后正常但端到端可见延迟持续增长——第三层信号会兜住它，联系 Relyt 管理员
  处置。

**不建议**用 staging 目录的文件数量做滞后信号：已消费的文件默认会保留
7 天（`gc_retain_days`）才清理，文件存在不代表未消费，堆积量与滞后没有
稳定的对应关系。
