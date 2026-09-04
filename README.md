# airouter-proxy

[![Release](https://github.com/ycvk/airouter-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/ycvk/airouter-proxy/releases/latest)

一个小型 HTTP 反向代理，用于在请求转发到上游 API 前改写顶层 JSON 字段，并流式返回上游响应。

## 快速开始

下载最新版预编译二进制，按平台选一条：

```bash
# macOS (Apple Silicon)
curl -fsSL https://github.com/ycvk/airouter-proxy/releases/latest/download/airouter-proxy-macos-arm64.tar.gz | tar xz

# Linux x86_64
curl -fsSL https://github.com/ycvk/airouter-proxy/releases/latest/download/airouter-proxy-linux-x86_64.tar.gz | tar xz
```

直接运行，上游地址和 key 用环境变量给：

```bash
UPSTREAM=https://api.example.com API_KEY=sk-example ./airouter-proxy
```

不放 `config.json` 也能跑，此时只转发并补 `Authorization`，不改写请求体；要改写字段见[配置](#配置)。

验证：

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{"model":"example","max_tokens":1024,"stream":true}'
```

## 功能

- 保留请求 method、path 和 query。
- 按 `rename` 重命名顶层 JSON 字段，再浅合并 `patch`。
- `patch` 中值为 `null` 的字段会从请求体删除。
- 只解析请求体顶层字段，嵌套内容按原始字节透传（不重排 key、不规范化数字）。
- 客户端未提供 `Authorization` 时，使用配置的 API key。
- `User-Agent` 统一改写为桌面 Chrome，客户端原值不透传。
- 上游请求固定 `Accept-Encoding: identity`，客户端的压缩偏好不透传：上游一旦 gzip，SSE 会被压缩器攒成一整块、等流结束才返回。代价是上游链路多传字节。
- 过滤逐跳 HTTP headers（含 `Connection` 动态列出的头），流式转发 SSE/chunked 响应。
- 上游连接失败时返回 502，body 为 `upstream error: <原因>`。
- 单个请求体最大 16 MiB。
- 上游连接超时 10s；不设整体超时，避免中断 SSE 长连接。
- 配置 `concurrency` 后限制同时打到上游的请求数，超出的请求排队等额度，等满 `queue_timeout`（默认 30s）返回 503 和 `Retry-After: 5`。
- 额度到响应体读完才归还，所以 SSE 全程占用。客户端断开（收到 FIN）立即中止请求：排队中的直接出队、不打上游；正在流的连上游连接一起断。代价是不再支持 HTTP 半关闭（发完请求就 shutdown 写半边、再等响应的客户端）。
- 上游启用 HTTP/2，多个流复用一条连接（按 ALPN 协商，上游不支持时回落 HTTP/1.1）。

## 手动下载

Windows 或需要指定版本时，到 [Releases](https://github.com/ycvk/airouter-proxy/releases/latest) 下载：

| 平台 | 文件 |
| --- | --- |
| Linux x86_64 | `airouter-proxy-linux-x86_64.tar.gz` |
| macOS Apple Silicon | `airouter-proxy-macos-arm64.tar.gz` |
| Windows x86_64 | `airouter-proxy-windows-x86_64.zip` |

解压后 `cd` 进解压目录，创建 `config.json`（格式参见 [`config.example.json`](config.example.json)），再运行 `airouter-proxy`（Windows 为 `airouter-proxy.exe`）。配置文件按运行目录（cwd）查找，不是二进制所在目录。

Linux 包是 musl 静态链接的，不依赖系统 glibc 版本，任何发行版解压即可运行。

## 从源码运行

需要 Rust stable toolchain。

```bash
cp config.example.json config.json
cargo run --release
```

默认监听 `0.0.0.0:8080`。`config.json` 已被 Git 忽略。

调试模式把请求体、响应状态和上游协商到的 HTTP 版本写到 stderr：

```bash
cargo run --release -- --debug
```

## 配置

参见 [`config.example.json`](config.example.json)：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `port` | 否 | 监听端口，范围 `1..=65535`，默认 `8080`。 |
| `upstream` | 是 | 上游 `http://` 或 `https://` 地址，不允许 query 或 fragment；末尾 `/` 会被去掉。 |
| `api_key` | 否 | 客户端未发送 `Authorization` 时使用。 |
| `concurrency` | 否 | 同时转发到上游的最大请求数，至少 `1`；不设则不限。超出的请求先排队，见 `queue_timeout`。 |
| `queue_timeout` | 否 | 排队等额度的秒数，默认 `30`；`0` 表示不排队，没额度直接 503。只在设了 `concurrency` 时可用。 |
| `patch` | 否 | 浅合并到请求体顶层的 JSON object；值为 `null` 时删除字段。 |
| `rename` | 否 | 顶层字段重命名映射，所有目标值必须是字符串。 |

处理顺序固定为 `rename` 后 `patch`。例如：

```json
{
  "patch": {
    "temperature": 0.7,
    "stream": null
  },
  "rename": {
    "max_tokens": "max_completion_tokens"
  }
}
```

环境变量会覆盖同名文件配置：

```bash
PORT=8080 \
UPSTREAM=https://api.example.com \
API_KEY=sk-example \
CONCURRENCY=2 \
QUEUE_TIMEOUT=30 \
cargo run --release
```

可以不创建 `config.json`，但必须通过 `UPSTREAM` 提供上游地址。配置格式、端口或 URL 无效时，程序会直接退出。

## 构建与检查

```bash
cargo build --release
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt -- --check
```

发布二进制位于 `target/release/airouter-proxy`，release profile 已开启 `strip`（不带符号表，panic backtrace 无符号名）。

## 安全提示

- 默认绑定 `0.0.0.0` 且没有入站鉴权：任何能访问该端口的人都能借用配置的 `api_key`。只在受信网络暴露，或改绑 `127.0.0.1`。
- 不要提交包含真实凭据的 `config.json`。
- `--debug` 会记录完整请求体，只应在受控环境中使用。
- 客户端自己提供的 `Authorization` 优先于配置的 `api_key`。

## 许可

[MIT](LICENSE)
