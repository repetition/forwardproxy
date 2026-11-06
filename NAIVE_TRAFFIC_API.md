# Naive Traffic API

其它节点按本约定实现 `/internal/naive/traffic`，即可对接同一套采集端。本文描述当前 `caddy-forwardproxy`（`naive` 分支）的实际行为。

## 1. 服务形态

| 项 | 约定 |
|---|---|
| 协议 | HTTP/1.1，明文 |
| 默认监听 | `127.0.0.1:9199` |
| 环境变量 | `NAIVE_TRAFFIC_ADDR`，覆盖监听地址（例如 `:9199` 或 `127.0.0.1:9199`） |
| 路径 | `/internal/naive/traffic` |
| 鉴权 | **无**。只应绑本机 loopback，不要对公网开放 |
| 读写超时 | 读 5s / 写 30s |
| 方法 | `GET`（非 `GET` 请求返回 `405 Method Not Allowed`） |

```http
GET /internal/naive/traffic HTTP/1.1
Host: 127.0.0.1:9199
```

```http
GET /internal/naive/traffic?reset=true HTTP/1.1
Host: 127.0.0.1:9199
```

```bash
curl -s http://127.0.0.1:9199/internal/naive/traffic
curl -s "http://127.0.0.1:9199/internal/naive/traffic?reset=true"
```

## 2. Query

| 参数 | 类型 | 默认 | 语义 |
|---|---|---|---|
| `reset` | string | 空（不重置） | 仅当值为 **`true` 或 `1`** 时做原子读并清零。`false`、空、其它值都不清零 |

建议采集侧周期调用 `?reset=true`，把返回值当作「本周期增量」。

## 3. 响应

- HTTP 状态：始终 **200**（即使没有用户也是 200）
- Header：`Content-Type: application/json`
- Body：JSON object，**顶层 key = 用户名**
- 空结果：`{}`（Go `json.Encoder` 末尾会多一个 `\n`）

### 3.1 顶层结构

```json
{
  "<username>": {
    "upload": 0,
    "download": 0,
    "connections": 0,
    "last_active": 0,
    "domains": {
      "<domain>": { "up": 0, "down": 0 }
    }
  }
}
```

### 3.2 用户字段

| JSON 字段 | 类型 | 是否会被 reset 清零 | 含义 |
|---|---|---|---|
| `upload` | int64 | 是 | 该用户发往目标的字节数（客户端 → 目标） |
| `download` | int64 | 是 | 该用户从目标收到的字节数（目标 → 客户端） |
| `connections` | int64 | **否** | 当前仍打开的隧道数；`Close` 只减一次 |
| `last_active` | int64 | **否** | 最近一次有字节读写的 Unix 秒（`time.Now().Unix()`） |
| `domains` | object | 见下 | 按目标主机拆分；`omitempty`：没有可展示域名时整段省略 |

### 3.3 域名字段

| JSON 字段 | 类型 | 是否会被 reset 清零 | 含义 |
|---|---|---|---|
| `up` | int64 | 是 | 该用户访问该目标的上传字节 |
| `down` | int64 | 是 | 该用户访问该目标的下载字节 |

同一用户下：`upload` ≈ 各 domain `up` 之和，`download` ≈ 各 domain `down` 之和（并发下允许瞬时不完全相等）。采集端以用户级字段为准做计费，`domains` 做拆分。

## 4. Key 规则

### 用户名

- 来自代理 `Proxy-Authorization: Basic ...` 解码后的 username。
- 未认证 / 空用户名 → 固定写成 **`_anonymous_`**。
- 用户条目创建后**不会删除**；reset 只清计数，不删用户。

### 域名（`domains` 的 key）

- 使用 CONNECT 目标的 `host:port`（优先 `r.URL.Host`，为空再用 `r.Host`），经 `normalizeDomain` 规范化后作为 key。
- 规范化规则：**剥离端口**、转小写、去除末尾 `.`（例如 `Example.COM.:443` → `example.com`，`[2001:db8::1]:443` → `2001:db8::1`）。
- 典型：`example.com`、`www.google.com`、`1.1.1.1`。
- 目标 host 为空或仅含端口（如 `""`、`":443"`） → **`_unknown_`**。
- 单用户独立域名上限为 `2000`（`maxDomainsPerUser`），超出上限的新域名统一归入 **`_others_`**。

## 5. `reset` 语义

对每个用户：

1. 用户 `upload` / `download`：原子 `Swap(0)`，**返回值是本周期增量**，内存变为 0。
2. 每个 domain 同样 `Swap(0)`。
3. 若该 domain 换出来的 `up == 0 && down == 0`，**立刻从 map 删除该 domain**（防域名无限膨胀）。
4. 快照里只放入 `up > 0 || down > 0` 的 domain。
5. **用户是否出现在 JSON 里**：
   - `reset=false`：所有已出现过的用户都返回（即使全 0）。
   - `reset=true`：仅当 `upload>0 || download>0 || connections>0 || 有非零 domain` 才返回该用户。  
     因此「空闲用户、无连接、本周期 0 流量」在 reset 响应里会被省略。

注意：第一次 `reset=true` 会返回本周期流量并把计数清 0，但 domain key 还在（值为 0）；**下一次** `reset=true` 才会因 `0/0` 删掉该 domain。实现侧应复现这个「二次 reset 才驱逐」行为，或至少保证 idle domain 最终会被删掉。

`connections`、`last_active` 永不因 reset 清零。

## 6. 字节方向

只统计隧道 payload（对目标连接 Read/Write 成功字节）：

- `Write` 到目标 → `upload` / domain `up`
- `Read` 自目标 → `download` / domain `down`
- `n == 0` 不计
- 建连时 `connections += 1`，`Close` 时 `connections -= 1`（只减一次）
- 有实际读写时更新 `last_active`

HTTP/1.1 Hijack、HTTP/2 CONNECT、HTTP/3 都走同一套。

## 7. 完整示例

### 查询（不清零）

```http
GET /internal/naive/traffic HTTP/1.1
```

```json
{
  "alice": {
    "upload": 19,
    "download": 27,
    "connections": 2,
    "last_active": 1762396800,
    "domains": {
      "example.com": { "up": 19, "down": 27 }
    }
  },
  "bob": {
    "upload": 1024,
    "download": 4096,
    "connections": 0,
    "last_active": 1700000000,
    "domains": {
      "github.com": { "up": 100, "down": 200 }
    }
  }
}
```

### 读并清零

```http
GET /internal/naive/traffic?reset=true HTTP/1.1
```

返回结构相同，`upload` / `download` / `domains.*.up|down` 是本周期增量。随后内存中用户级 up/down 为 0；domain 若本周期有流量则仍在 map 里值为 0，下一次 reset 再删。

### 无数据

```json
{}
```

## 8. JSON Schema

```json
{
  "type": "object",
  "additionalProperties": {
    "type": "object",
    "required": ["upload", "download", "connections", "last_active"],
    "properties": {
      "upload": { "type": "integer" },
      "download": { "type": "integer" },
      "connections": { "type": "integer" },
      "last_active": { "type": "integer", "description": "unix seconds" },
      "domains": {
        "type": "object",
        "additionalProperties": {
          "type": "object",
          "required": ["up", "down"],
          "properties": {
            "up": { "type": "integer" },
            "down": { "type": "integer" }
          }
        }
      }
    }
  }
}
```

采集端类型示例：

```ts
type DomainSnap = { up: number; down: number };
type UserSnap = {
  upload: number;
  download: number;
  connections: number;
  last_active: number;
  domains?: Record<string, DomainSnap>;
};
type TrafficResponse = Record<string, UserSnap>;
```

## 9. 对接注意

1. 数字用 **int64**，不要用 float。
2. 并发安全：计数用原子加；reset 用原子 swap。
3. 无 CORS、无 cookie、无 TLS（本机接口）。
4. 不要依赖固定用户顺序；JSON object 无序。
5. `domains` 可能缺失，按 `{}` 处理。
6. 用户 key 是字符串，不要假设一定是邮箱或 UUID。
7. 计费请用用户级 `upload` / `download`；`domains` 只做明细。
8. 轮询间隔由采集端决定；reset 后增量不会在两次 reset 之间重复计入（原子 swap）。
