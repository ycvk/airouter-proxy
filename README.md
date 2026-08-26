# airouter-proxy

[![Release](https://github.com/ycvk/airouter-proxy/actions/workflows/release.yml/badge.svg)](https://github.com/ycvk/airouter-proxy/releases/latest)

一个小型 HTTP 反向代理，用于在请求转发到上游 API 前改写顶层 JSON 字段，并流式返回上游响应。

## 功能

- 保留请求 method、path 和 query。
- 按 `rename` 重命名顶层 JSON 字段，再浅合并 `patch`。
- `patch` 中值为 `null` 的字段会从请求体删除。
- 只解析请求体顶层字段，嵌套内容按原始字节透传（不重排 key、不规范化数字）。
- 客户端未提供 `Authorization` 时，使用配置的 API key。
- 过滤逐跳 HTTP headers，流式转发 SSE/chunked 响应。
- 单个请求体最大 16 MiB。
- 上游连接超时 10s；不设整体超时，避免中断 SSE 长连接。
- 上游启用 HTTP/2，多个流复用一条连接（按 ALPN 协商，上游不支持时回落 HTTP/1.1）。

## 下载

从 [Latest Release](https://github.com/ycvk/airouter-proxy/releases/latest) 下载对应平台的预编译包：

| 平台 | 文件 |
| --- | --- |
| Linux x86_64 | `airouter-proxy-linux-x86_64.tar.gz` |
| macOS Apple Silicon | `airouter-proxy-macos-arm64.tar.gz` |
| Windows x86_64 | `airouter-proxy-windows-x86_64.zip` |

解压后，在二进制同目录创建 `config.json`，配置格式参见 [`config.example.json`](config.example.json)，然后直接运行 `airouter-proxy`（Windows 为 `airouter-proxy.exe`）。

## 从源码运行

需要 Rust stable toolchain。

```bash
cp config.example.json config.json
cargo run --release
```

默认监听 `0.0.0.0:8080`。`config.json` 已被 Git 忽略。

调试模式会把请求体写到 stderr：

```bash
cargo run --release -- --debug
```

请求示例：

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer client-key' \
  -d '{"model":"example","max_tokens":1024,"stream":true}'
```

## 配置

参见 [`config.example.json`](config.example.json)：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `port` | 否 | 监听端口，范围 `1..=65535`，默认 `8080`。 |
| `upstream` | 是 | 上游 `http://` 或 `https://` 地址，不允许 query 或 fragment。 |
| `api_key` | 否 | 客户端未发送 `Authorization` 时使用。 |
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

发布二进制位于 `target/release/airouter-proxy`。

## 安全提示

- 不要提交包含真实凭据的 `config.json`。
- `--debug` 会记录完整请求体，只应在受控环境中使用。
- 客户端自己提供的 `Authorization` 优先于配置的 `api_key`。
