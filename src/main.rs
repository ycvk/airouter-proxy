use actix_web::body::{BodySize, BodyStream, MessageBody};
use actix_web::http::{header, StatusCode};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::Value;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// 上游可能按 UA 区分浏览器/客户端流量, 用真实 Chrome UA 而非 reqwest 默认
const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36";

struct Cfg {
    port: u16,
    upstream: String,
    /// 顶层字段改名 (from, to)
    rename: Vec<(String, String)>,
    /// 顶层浅合并, None 表示删除该 key; 预转成 RawValue, 热路径直接拼字节
    patch: Vec<(String, Option<Box<RawValue>>)>,
    debug: bool,
    api_key: Option<String>,
    /// 并发上限; None 表示不限。permit 随响应体一起 drop, 见 PermitBody
    limit: Option<Arc<Semaphore>>,
    /// 排队等额度的上限, 超时返回 503; 0 表示不排队, 没额度就直接 503
    queue_timeout: Duration,
    client: reqwest::Client,
}

#[derive(Default, Deserialize)]
struct FileCfg {
    port: Option<u16>,
    upstream: Option<String>,
    concurrency: Option<u32>,
    queue_timeout: Option<u64>,
    #[serde(default)]
    patch: Value,
    #[serde(default)]
    rename: Value,
    api_key: Option<String>,
}

/// 配置: cwd 下的 config.json, 环境变量 PORT/UPSTREAM/API_KEY 可覆盖。
fn load_cfg() -> Result<Cfg, String> {
    let file_cfg = match std::fs::read_to_string("config.json") {
        Ok(s) => parse_file_cfg(&s)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => FileCfg::default(),
        Err(e) => return Err(format!("cannot read config.json: {e}")),
    };
    let mut port = file_cfg.port.unwrap_or(8080);
    let mut upstream = file_cfg.upstream;
    let mut api_key = file_cfg.api_key;
    let mut concurrency = file_cfg.concurrency;
    let mut queue_timeout = file_cfg.queue_timeout;

    if let Some(value) = env_value("PORT")? {
        port = value
            .parse::<u16>()
            .map_err(|_| format!("invalid PORT {value:?}: expected 1..=65535"))?;
        if port == 0 {
            return Err("invalid PORT \"0\": expected 1..=65535".to_string());
        }
    }
    if let Some(value) = env_value("UPSTREAM")? {
        upstream = Some(value);
    }
    if let Some(value) = env_value("API_KEY")? {
        api_key = Some(value);
    }
    if let Some(value) = env_value("CONCURRENCY")? {
        concurrency = Some(
            value
                .parse::<u32>()
                .ok()
                .filter(|&limit| limit > 0)
                .ok_or_else(|| format!("invalid CONCURRENCY {value:?}: expected at least 1"))?,
        );
    }
    if let Some(value) = env_value("QUEUE_TIMEOUT")? {
        queue_timeout = Some(
            value
                .parse::<u64>()
                .map_err(|_| format!("invalid QUEUE_TIMEOUT {value:?}: expected seconds"))?,
        );
    }
    // 不限并发时排队超时是死配置, 直接报错而不是静默忽略
    if queue_timeout.is_some() && concurrency.is_none() {
        return Err(
            "queue_timeout/QUEUE_TIMEOUT needs concurrency/CONCURRENCY to be set".to_string(),
        );
    }

    let upstream = upstream.ok_or("missing upstream: set config.json upstream or UPSTREAM")?;
    let upstream = upstream.trim_end_matches('/').to_string();
    validate_upstream(&upstream)?;
    if api_key.as_deref().is_some_and(str::is_empty) {
        return Err("api_key/API_KEY must not be empty".to_string());
    }

    Ok(Cfg {
        port,
        upstream,
        rename: rename_pairs(&file_cfg.rename),
        patch: patch_pairs(&file_cfg.patch),
        debug: std::env::args().any(|a| a == "--debug"),
        api_key,
        limit: concurrency.map(|limit| Arc::new(Semaphore::new(limit as usize))),
        // 默认 30s: 排队没有上限的话, 等待者会攥着已缓冲的请求体一直堆积
        queue_timeout: Duration::from_secs(queue_timeout.unwrap_or(30)),
        client: reqwest::Client::builder()
            // 不设整体 timeout: 会误杀长 SSE 流; 只兜住连不上和空闲池连接
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .build()
            // reqwest 的 Display 只有 "builder error"; Debug 带上 source 链, 最常见的原因是
            // 系统根证书缺失(最小化容器/镜像), rustls 拿不到 TLS root store。
            .map_err(|e| {
                format!("cannot build http client: {e:?}; missing system CA certificates? install ca-certificates")
            })?,
    })
}

fn parse_file_cfg(s: &str) -> Result<FileCfg, String> {
    let cfg =
        serde_json::from_str::<FileCfg>(s).map_err(|e| format!("invalid config.json: {e}"))?;
    if cfg.port == Some(0) {
        return Err("invalid config.json port: expected 1..=65535".to_string());
    }
    if cfg.concurrency == Some(0) {
        return Err("invalid config.json concurrency: expected at least 1".to_string());
    }
    if cfg.upstream.as_deref().is_some_and(str::is_empty) {
        return Err("config.json upstream must not be empty".to_string());
    }
    if !cfg.patch.is_null() && !cfg.patch.is_object() {
        return Err("config.json patch must be an object or null".to_string());
    }
    if !cfg.rename.is_null() && !cfg.rename.is_object() {
        return Err("config.json rename must be an object or null".to_string());
    }
    if cfg
        .rename
        .as_object()
        .is_some_and(|rename| rename.values().any(|value| !value.is_string()))
    {
        return Err("config.json rename values must be strings".to_string());
    }
    if cfg.api_key.as_deref().is_some_and(str::is_empty) {
        return Err("config.json api_key must not be empty".to_string());
    }
    Ok(cfg)
}

fn env_value(name: &str) -> Result<Option<String>, String> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(e) => Err(format!("invalid {name}: {e}")),
    }
}

fn validate_upstream(upstream: &str) -> Result<(), String> {
    let url = reqwest::Url::parse(upstream).map_err(|e| format!("invalid upstream URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err("upstream must be an absolute http:// or https:// URL".to_string());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("upstream must not contain a query or fragment".to_string());
    }
    Ok(())
}

/// config 的 rename/patch 预转成热路径可直接用的形式, 避免每请求重新走 Value。
fn rename_pairs(rename: &Value) -> Vec<(String, String)> {
    let Some(map) = rename.as_object() else {
        return Vec::new();
    };
    map.iter()
        .filter_map(|(from, to)| to.as_str().map(|to| (from.clone(), to.to_string())))
        .collect()
}

fn patch_pairs(patch: &Value) -> Vec<(String, Option<Box<RawValue>>)> {
    let Some(map) = patch.as_object() else {
        return Vec::new();
    };
    map.iter()
        .map(|(k, val)| {
            // 已解析的 Value 不含 NaN/Inf, 重新序列化不会失败
            let raw = (!val.is_null())
                .then(|| serde_json::value::to_raw_value(val).expect("patch value re-serializes"));
            (k.clone(), raw)
        })
        .collect()
}

/// 顶层改写: 先按 rename 改名, 再浅合并 patch(None 表示删除该 key)。
/// 只解析顶层 key, 嵌套内容按原始字节透传(不重排 key、不规范化数字字面量)。
/// 返回 None 表示无需改写, 调用方零拷贝转发原 body。
fn rewrite_body<'a>(
    body: &'a [u8],
    rename: &'a [(String, String)],
    patch: &'a [(String, Option<Box<RawValue>>)],
) -> Option<Vec<u8>> {
    if rename.is_empty() && patch.is_empty() {
        return None;
    }
    let mut obj: BTreeMap<String, &'a RawValue> = serde_json::from_slice(body).ok()?;
    for (from, to) in rename {
        if let Some(val) = obj.remove(from.as_str()) {
            obj.insert(to.clone(), val);
        }
    }
    for (k, val) in patch {
        match val {
            Some(raw) => obj.insert(k.clone(), raw),
            None => obj.remove(k.as_str()),
        };
    }
    serde_json::to_vec(&obj).ok()
}

// reqwest 按改写后的 body 和上游 URL 重建 content-length/host。
fn is_request_skipped(name: &str) -> bool {
    is_hop_by_hop(name) || matches!(name, "content-length" | "host")
}

// RFC 9110 §7.6.1 固定逐跳头; Connection 还可动态指定其他逐跳头。
fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn connection_lists_header(value: &str, name: &str) -> bool {
    value
        .split(',')
        .any(|candidate| candidate.trim().eq_ignore_ascii_case(name))
}

fn is_connection_named<'a>(mut values: impl Iterator<Item = &'a str>, name: &str) -> bool {
    values.any(|value| connection_lists_header(value, name))
}

/// 把并发额度绑在响应体上: 响应体 drop 时才归还, 所以上游还在吐流的请求一直占额度。
/// 客户端中途断开只有在 actix 下一次写响应失败时才被发现(静默的流会占到上游自己结束)。
/// 只在 poll_next 上转发, 所以内层 body 必须 Unpin(BodyStream 对 Unpin 的流是 Unpin)。
struct PermitBody<B> {
    inner: B,
    _permit: OwnedSemaphorePermit,
}

impl<B: MessageBody + Unpin> MessageBody for PermitBody<B> {
    type Error = B::Error;

    fn size(&self) -> BodySize {
        self.inner.size()
    }

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<web::Bytes, Self::Error>>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

async fn proxy(req: HttpRequest, body: web::Bytes) -> Result<HttpResponse, actix_web::Error> {
    let cfg = req
        .app_data::<web::Data<Cfg>>()
        .expect("cfg registered")
        .clone();

    // path_and_query 保留 query, 并剥掉 absolute-form 的 scheme+authority
    let path = req.uri().path_and_query().map_or("/", |pq| pq.as_str());
    // 未命中改写时原 Bytes 直接下发(与 reqwest 共用 bytes crate), 不多拷一份
    let rewritten = rewrite_body(&body, &cfg.rename, &cfg.patch);
    if cfg.debug {
        let out = rewritten.as_deref().unwrap_or(&body);
        eprintln!(
            "-> {} {} body={}B: {}",
            req.method(),
            path,
            out.len(),
            String::from_utf8_lossy(out)
        );
    }
    let body = match rewritten {
        Some(rewritten) => reqwest::Body::from(rewritten),
        None => reqwest::Body::from(body),
    };

    // ponytail: 字符串桥接 actix(http 0.2) 与 reqwest(http 1.x) 的 header 类型
    let mut headers = reqwest::header::HeaderMap::with_capacity(req.headers().len() + 2);
    let mut has_auth = false;
    // Connection 列出的头也是逐跳的; 一次取出复用, 不在每个 header 上重查
    let conn_tokens: Vec<&str> = req
        .headers()
        .get_all(header::CONNECTION)
        .filter_map(|value| value.to_str().ok())
        .collect();
    for (k, v) in req.headers().iter() {
        let name = k.as_str();
        if is_request_skipped(name) || is_connection_named(conn_tokens.iter().copied(), name) {
            continue;
        }
        let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_bytes()) else {
            continue; // 非法 header 名丢弃
        };
        let Ok(val) = reqwest::header::HeaderValue::from_bytes(v.as_bytes()) else {
            continue; // 非法 header 值丢弃
        };
        has_auth = has_auth || name == reqwest::header::AUTHORIZATION;
        headers.append(name, val);
    }
    // 客户端没带 Authorization 时用配置的 key
    if !has_auth {
        if let Some(key) = &cfg.api_key {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {key}").parse().expect("static format"),
            );
        }
    }
    // 无条件覆盖为真实浏览器 UA, 客户端带的 UA 不向上游透传
    headers.insert(
        reqwest::header::USER_AGENT,
        BROWSER_UA.parse().expect("static UA"),
    );

    let permit = match &cfg.limit {
        Some(sem) => {
            let queued = Instant::now();
            let Ok(permit) =
                tokio::time::timeout(cfg.queue_timeout, sem.clone().acquire_owned()).await
            else {
                if cfg.debug {
                    eprintln!(
                        "!! 503 {path}: queued {:?} without a slot",
                        cfg.queue_timeout
                    );
                }
                return Ok(HttpResponse::ServiceUnavailable()
                    .content_type("text/plain")
                    // 纯提示: 上游什么时候放出额度我们并不知道
                    .insert_header((header::RETRY_AFTER, "5"))
                    .body(format!(
                        "concurrency limit: queued {:?} without a slot",
                        cfg.queue_timeout
                    )));
            };
            let waited = queued.elapsed();
            if cfg.debug && waited > Duration::from_millis(1) {
                eprintln!("~~ queued {waited:.2?} {path}");
            }
            Some(permit.expect("semaphore never closed"))
        }
        None => None,
    };

    let url = format!("{}{}", cfg.upstream, path);
    let res = match cfg
        .client
        .request(req.method().as_str().parse().expect("method"), &url)
        .headers(headers)
        .body(body)
        .send()
        .await
    {
        Ok(res) => res,
        Err(e) => {
            return Ok(HttpResponse::BadGateway()
                .content_type("text/plain")
                .body(format!("upstream error: {e}")));
        }
    };

    let status = StatusCode::from_u16(res.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if cfg.debug {
        eprintln!("<- {status} {:?} {path}", res.version());
    }
    let mut resp = HttpResponse::build(status);
    {
        // 同样一次取出 Connection token; Vec 借用 res.headers(), 块结束后才能 move res
        let conn_tokens: Vec<&str> = res
            .headers()
            .get_all(reqwest::header::CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .collect();
        for (k, v) in res.headers().iter() {
            let name = k.as_str();
            // BodyStream 重新生成传输 framing；reqwest 未启用解压 feature，保留 content-encoding。
            if is_hop_by_hop(name)
                || name == "content-length"
                || is_connection_named(conn_tokens.iter().copied(), name)
            {
                continue;
            }
            // 非法 header 值丢弃, 不阻断响应
            if let Ok(val) = header::HeaderValue::from_bytes(v.as_bytes()) {
                resp.append_header((name, val));
            }
        }
    }
    // 流式转发: SSE / chunked 响应按块透传, 不等整个 body
    let body = BodyStream::new(res.bytes_stream());
    Ok(match permit {
        // 额度跟着 body 走: 上游还在吐流的请求一直占额度
        Some(permit) => resp.body(PermitBody {
            inner: body,
            _permit: permit,
        }),
        None => resp.body(body),
    })
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // 直接打印, 不套 io::Error::other 的 Custom { kind: Other, error: ".." } 包装
    let cfg = match load_cfg() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };
    if cfg.debug {
        eprintln!("debug: on (request logs on stderr)");
    }
    for (from, to) in &cfg.rename {
        println!("rename: {from} -> {to}");
    }
    for (k, val) in &cfg.patch {
        println!(
            "patch: {k} = {}",
            val.as_deref().map_or("<removed>", RawValue::get)
        );
    }
    if let Some(sem) = &cfg.limit {
        // 启动时一个 permit 都没发出去, available 就是配置值
        println!(
            "concurrency: {} (queue timeout {:?})",
            sem.available_permits(),
            cfg.queue_timeout
        );
    }

    let (port, upstream) = (cfg.port, cfg.upstream.clone());
    // Data 只建一次, 各 worker clone 的是 Arc, 不再每 worker 深拷一份配置
    let data = web::Data::new(cfg);
    let app = HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .app_data(web::PayloadConfig::new(16 << 20)) // ponytail: 长对话 JSON 常超默认 256KB; 16MB 足够, 再大按需调
            .service(web::resource("/{path:.*}").route(web::to(proxy)))
    })
    // 客户端 FIN 即中止请求: 默认允许半关闭, 排队中/流式的请求会在客户端走后继续跑,
    // 白占并发额度还白烧上游 token。代价是真半关闭(发完请求就 shutdown 写半边)的客户端不再支持。
    .h1_allow_half_closed(false)
    .bind(("0.0.0.0", port))?; // 0.0.0.0: 供 OrbStack 容器内 pentagi 经 orb.local (VM 网关) 访问

    println!("listening on http://127.0.0.1:{port}  ->  {upstream}");
    app.run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn test_cfg(upstream: String) -> Cfg {
        Cfg {
            port: 8080,
            upstream,
            rename: rename_pairs(&serde_json::json!({"max_tokens": "max_completion_tokens"})),
            patch: patch_pairs(&serde_json::json!({"reasoning_effort": "high"})),
            debug: false,
            api_key: Some("test-key".to_string()),
            limit: None,
            queue_timeout: Duration::from_secs(30),
            client: reqwest::Client::new(),
        }
    }

    fn raw_header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().skip(1).find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    fn probe_request(uri: &str) -> actix_web::test::TestRequest {
        actix_web::test::TestRequest::post()
            .uri(uri)
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header((header::CONNECTION, "x-client-hop"))
            .insert_header(("x-client-hop", "secret"))
            .insert_header((header::TE, "trailers"))
            .insert_header(("x-request-id", "request-42"))
            .set_payload(r#"{"model":"demo","max_tokens":128}"#)
    }

    /// 读一个完整请求(头 + Content-Length 指定长度的 body)
    fn read_request(stream: &mut std::net::TcpStream) -> (String, Vec<u8>) {
        use std::io::Read;

        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut request = Vec::new();
        let mut buf = [0; 4096];
        let body_start = loop {
            if let Some(offset) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break offset + 4;
            }
            let read = stream.read(&mut buf).unwrap();
            assert!(read > 0, "upstream request ended before its headers");
            request.extend_from_slice(&buf[..read]);
        };
        let head = std::str::from_utf8(&request[..body_start])
            .unwrap()
            .to_owned();
        let content_length = raw_header(&head, "content-length")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        while request.len() < body_start + content_length {
            let read = stream.read(&mut buf).unwrap();
            assert!(read > 0, "upstream request ended before its body");
            request.extend_from_slice(&buf[..read]);
        }
        (
            head,
            request[body_start..body_start + content_length].to_vec(),
        )
    }

    fn spawn_asserting_upstream() -> (String, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            use std::io::Write;

            let (mut stream, _) = listener.accept().unwrap();
            let (head, body) = read_request(&mut stream);

            assert!(head.starts_with("POST /v1/chat/completions?probe=1 HTTP/1.1\r\n"));
            assert_eq!(raw_header(&head, "authorization"), Some("Bearer test-key"));
            assert_eq!(raw_header(&head, "user-agent"), Some(BROWSER_UA));
            assert_eq!(raw_header(&head, "x-request-id"), Some("request-42"));
            assert_eq!(raw_header(&head, "connection"), None);
            assert_eq!(raw_header(&head, "x-client-hop"), None);
            assert_eq!(raw_header(&head, "te"), None);
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["model"], "demo");
            assert_eq!(body["max_completion_tokens"], 128);
            assert_eq!(body["reasoning_effort"], "high");

            let response_body = br#"{"ok":true}"#;
            write!(
                stream,
                "HTTP/1.1 201 Created\r\nConnection: x-upstream-hop\r\nX-Upstream-Hop: secret\r\nContent-Encoding: gzip\r\nX-Upstream: ok\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                response_body.len()
            )
            .unwrap();
            stream.write_all(response_body).unwrap();
        });
        (format!("http://{addr}"), thread)
    }

    /// 一条连接处理一个请求, 返回上游侧同时在处理的请求数峰值。
    fn spawn_counting_upstream(
        requests: usize,
    ) -> (String, Arc<AtomicUsize>, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let peak = Arc::new(AtomicUsize::new(0));
        let thread = std::thread::spawn({
            let peak = peak.clone();
            move || {
                let inflight = Arc::new(AtomicUsize::new(0));
                let workers: Vec<_> = (0..requests)
                    .map(|_| {
                        let (mut stream, _) = listener.accept().unwrap();
                        let (inflight, peak) = (inflight.clone(), peak.clone());
                        std::thread::spawn(move || {
                            use std::io::Write;

                            read_request(&mut stream);
                            let now = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            // 攥住一会儿, 让真正并发的请求在上游侧重叠
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            // Connection: close 让 reqwest 不复用连接, 一条连接对应一个请求
                            let response =
                                b"HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nok";
                            stream.write_all(response).unwrap();
                            inflight.fetch_sub(1, Ordering::SeqCst);
                        })
                    })
                    .collect();
                for worker in workers {
                    worker.join().unwrap();
                }
            }
        });
        (format!("http://{addr}"), peak, thread)
    }

    #[test]
    fn invalid_config_is_rejected() {
        for config in [
            "{",
            r#"{"port":0}"#,
            r#"{"port":70000}"#,
            r#"{"patch":[]}"#,
            r#"{"rename":{"old":1}}"#,
            r#"{"api_key":""}"#,
        ] {
            assert!(parse_file_cfg(config).is_err(), "accepted {config}");
        }
        assert!(validate_upstream("ftp://example.com").is_err());
        assert!(validate_upstream("https://example.com?token=x").is_err());
    }

    #[actix_web::test]
    async fn proxy_rewrites_request_and_filters_hop_headers() {
        // origin-form, 以及客户端把本代理当 HTTP proxy 时的 absolute-form, 都必须以 origin-form 发给上游
        for uri in [
            "/v1/chat/completions?probe=1",
            "http://proxy.invalid/v1/chat/completions?probe=1",
        ] {
            let (upstream, upstream_thread) = spawn_asserting_upstream();
            let app = actix_web::test::init_service(
                App::new()
                    .app_data(web::Data::new(test_cfg(upstream)))
                    .service(web::resource("/{path:.*}").route(web::to(proxy))),
            )
            .await;
            let response =
                actix_web::test::call_service(&app, probe_request(uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::CREATED, "uri {uri}");
            assert_eq!(
                response
                    .headers()
                    .get("x-upstream")
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "ok"
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::CONTENT_ENCODING)
                    .unwrap()
                    .to_str()
                    .unwrap(),
                "gzip"
            );
            assert!(!response.headers().contains_key(header::CONNECTION));
            assert!(!response.headers().contains_key("x-upstream-hop"));
            let payload: Value =
                serde_json::from_slice(&actix_web::test::read_body(response).await).unwrap();
            assert_eq!(payload["ok"], true);
            upstream_thread.join().unwrap();
        }
    }

    #[actix_web::test]
    async fn proxy_returns_bad_gateway_for_upstream_error() {
        let app = actix_web::test::init_service(
            App::new()
                .app_data(web::Data::new(test_cfg("http://127.0.0.1:0".to_string())))
                .service(web::resource("/{path:.*}").route(web::to(proxy))),
        )
        .await;
        let request = actix_web::test::TestRequest::post()
            .uri("/v1/chat/completions")
            .set_payload("{}")
            .to_request();
        let response = actix_web::test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = actix_web::test::read_body(response).await;
        assert!(std::str::from_utf8(&body)
            .unwrap()
            .starts_with("upstream error:"));
    }

    #[actix_web::test]
    async fn concurrency_limit_caps_inflight_upstream_requests() {
        let (upstream, peak, upstream_thread) = spawn_counting_upstream(4);
        let mut cfg = test_cfg(upstream);
        cfg.limit = Some(Arc::new(Semaphore::new(2)));
        let app = std::rc::Rc::new(
            actix_web::test::init_service(
                App::new()
                    .app_data(web::Data::new(cfg))
                    .service(web::resource("/{path:.*}").route(web::to(proxy))),
            )
            .await,
        );

        let calls: Vec<_> = (0..4)
            .map(|_| {
                let app = app.clone();
                actix_web::rt::spawn(async move {
                    let request = probe_request("/v1/chat/completions?probe=1").to_request();
                    let response = actix_web::test::call_service(&*app, request).await;
                    assert_eq!(response.status(), StatusCode::OK);
                    // 必须把 body 读完, 额度才随响应体归还
                    assert_eq!(actix_web::test::read_body(response).await, "ok");
                })
            })
            .collect();
        for call in calls {
            call.await.unwrap();
        }
        upstream_thread.join().unwrap();

        // 4 个并发请求, 上游最多同时看到 2 个; 额度提前归还会看到 3~4, 意外串行会是 1
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[actix_web::test]
    async fn queue_timeout_returns_service_unavailable() {
        // 上游是死地址: 真打上去会 502, 拿到 503 才说明请求根本没出门
        let mut cfg = test_cfg("http://127.0.0.1:0".to_string());
        let sem = Arc::new(Semaphore::new(1));
        cfg.limit = Some(sem.clone());
        cfg.queue_timeout = Duration::from_millis(20);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(web::Data::new(cfg))
                .service(web::resource("/{path:.*}").route(web::to(proxy))),
        )
        .await;

        let held = sem.acquire().await.unwrap();
        let request = probe_request("/v1/chat/completions").to_request();
        let response = actix_web::test::call_service(&app, request).await;

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(header::RETRY_AFTER)
                .unwrap()
                .to_str()
                .unwrap(),
            "5"
        );
        drop(held);
    }

    #[test]
    fn rename_field() {
        let out = rewrite_body(
            br#"{"model":"m","max_tokens":2048}"#,
            &rename_pairs(&serde_json::json!({"max_tokens": "max_completion_tokens"})),
            &[],
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert!(v.get("max_tokens").is_none());
        assert_eq!(v["max_completion_tokens"], 2048);
    }

    #[test]
    fn rename_then_patch() {
        // rename 先跑, 所以 patch 操作的是改名后的字段
        let out = rewrite_body(
            br#"{"max_tokens":100}"#,
            &rename_pairs(&serde_json::json!({"max_tokens": "max_completion_tokens"})),
            &patch_pairs(&serde_json::json!({"max_completion_tokens": 8192})),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["max_completion_tokens"], 8192);
        assert!(v.get("max_tokens").is_none());
    }

    #[test]
    fn patch_merge_and_remove() {
        let out = rewrite_body(
            br#"{"model":"old","stream":true}"#,
            &[],
            &patch_pairs(&serde_json::json!({"model": "new", "extra": 1, "stream": null})),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["model"], "new");
        assert_eq!(v["extra"], 1);
        assert!(v.get("stream").is_none());
    }

    #[test]
    fn nested_bytes_pass_through_untouched() {
        // 只解析顶层: 嵌套 key 顺序和数字字面量按原始字节透传
        let out = rewrite_body(
            br#"{"max_tokens":1,"tools":[{"z":1.50,"a":{"b":[3,2]}}]}"#,
            &rename_pairs(&serde_json::json!({"max_tokens": "max_completion_tokens"})),
            &[],
        )
        .unwrap();
        let out = std::str::from_utf8(&out).unwrap();
        assert!(out.contains(r#"{"z":1.50,"a":{"b":[3,2]}}"#), "{out}");
    }

    #[test]
    fn nothing_to_rewrite_returns_none() {
        // 空配置 / 非 JSON / 顶层非 object: 调用方原样零拷贝转发
        let patch = patch_pairs(&serde_json::json!({"model": "new"}));
        assert!(rewrite_body(br#"{"model":"old"}"#, &[], &[]).is_none());
        assert!(rewrite_body(b"not json", &[], &patch).is_none());
        assert!(rewrite_body(br#"[1,2]"#, &[], &patch).is_none());
        assert!(rewrite_body(
            br#"{"a":1}"#,
            &rename_pairs(&Value::Null),
            &patch_pairs(&serde_json::json!([1, 2])),
        )
        .is_none());
    }
}
