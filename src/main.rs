use actix_web::body::BodyStream;
use actix_web::http::{header, StatusCode};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use serde::Deserialize;
use serde_json::Value;

// RFC 9110 §7.6.1 固定逐跳头；Connection 还可动态指定其他逐跳头。
const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];
// reqwest 必须按改写后的 body 和上游 URL 重建这些头。
const REQUEST_REBUILT: [&str; 2] = ["content-length", "host"];
// 上游可能按 UA 区分浏览器/客户端流量, 用真实 Chrome UA 而非 reqwest 默认
const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36";

#[derive(Clone)]
struct Cfg {
    port: u16,
    upstream: String,
    patch: Value,
    rename: Value,
    debug: bool,
    api_key: Option<String>,
    client: reqwest::Client,
}

#[derive(Default, Deserialize)]
struct FileCfg {
    port: Option<u16>,
    upstream: Option<String>,
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

    let upstream = upstream.ok_or("missing upstream: set config.json upstream or UPSTREAM")?;
    let upstream = upstream.trim_end_matches('/').to_string();
    validate_upstream(&upstream)?;
    if api_key.as_deref().is_some_and(str::is_empty) {
        return Err("api_key/API_KEY must not be empty".to_string());
    }

    Ok(Cfg {
        port,
        upstream,
        patch: file_cfg.patch,
        rename: file_cfg.rename,
        debug: std::env::args().any(|a| a == "--debug"),
        api_key,
        client: reqwest::Client::new(),
    })
}

fn parse_file_cfg(s: &str) -> Result<FileCfg, String> {
    let cfg =
        serde_json::from_str::<FileCfg>(s).map_err(|e| format!("invalid config.json: {e}"))?;
    if cfg.port == Some(0) {
        return Err("invalid config.json port: expected 1..=65535".to_string());
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

/// 字段改名: rename {"max_tokens": "max_completion_tokens"} 把请求体顶层同名 key 改名。
fn apply_rename(body: &mut Vec<u8>, rename: &Value) {
    let Some(map) = rename.as_object() else {
        return;
    };
    if map.is_empty() {
        return;
    }
    let Ok(mut v) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let Some(obj) = v.as_object_mut() else {
        return;
    };
    for (from, to) in map {
        let Some(to) = to.as_str() else {
            continue;
        };
        if let Some(val) = obj.remove(from) {
            obj.insert(to.to_string(), val);
        }
    }
    if let Ok(b) = serde_json::to_vec(&v) {
        *body = b;
    }
}

/// 把 patch 浅合并进请求体顶层 JSON 对象; patch 值为 null 表示删除该 key。
fn apply_patch(body: &mut Vec<u8>, patch: &Value) {
    let Some(patch) = patch.as_object() else {
        return;
    };
    if patch.is_empty() {
        return;
    }
    let Ok(mut v) = serde_json::from_slice::<Value>(body) else {
        return;
    };
    let Some(obj) = v.as_object_mut() else {
        return;
    };
    for (k, val) in patch {
        if val.is_null() {
            obj.remove(k);
        } else {
            obj.insert(k.clone(), val.clone());
        }
    }
    if let Ok(b) = serde_json::to_vec(&v) {
        *body = b;
    }
}

fn is_request_skipped(name: &str) -> bool {
    is_hop_by_hop(name) || REQUEST_REBUILT.contains(&name)
}

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.contains(&name)
}

fn connection_lists_header(value: &str, name: &str) -> bool {
    value
        .split(',')
        .any(|candidate| candidate.trim().eq_ignore_ascii_case(name))
}

fn is_connection_named<'a>(mut values: impl Iterator<Item = &'a str>, name: &str) -> bool {
    values.any(|value| connection_lists_header(value, name))
}

async fn proxy(req: HttpRequest, body: web::Bytes) -> Result<HttpResponse, actix_web::Error> {
    let cfg = req
        .app_data::<web::Data<Cfg>>()
        .expect("cfg registered")
        .clone();

    // 原样保留 path + query, 例如 /v1/chat/completions?x=1
    let path = req.uri().to_string();
    let mut body = body.to_vec();
    // 先改名再 patch: renamed 后的字段名即为上游最终字段, patch 直接操作它
    apply_rename(&mut body, &cfg.rename);
    apply_patch(&mut body, &cfg.patch);
    if cfg.debug {
        eprintln!(
            "-> {} {} body={}B: {}",
            req.method(),
            path,
            body.len(),
            String::from_utf8_lossy(&body)
        );
    }

    // ponytail: 字符串桥接 actix(http 0.2) 与 reqwest(http 1.x) 的 header 类型
    let mut headers = reqwest::header::HeaderMap::new();
    let mut has_auth = false;
    // ponytail: header 数量很小；逐项重扫 Connection token，避免每请求分配集合。
    for (k, v) in req.headers().iter() {
        let name = k.as_str();
        if is_request_skipped(name)
            || is_connection_named(
                req.headers()
                    .get_all(header::CONNECTION)
                    .filter_map(|value| value.to_str().ok()),
                name,
            )
        {
            continue;
        }
        let Ok(name) = reqwest::header::HeaderName::from_bytes(name.as_bytes()) else {
            continue; // 非法 header 名丢弃
        };
        let Some(s) = v.to_str().ok() else {
            continue;
        };
        let Ok(val) = s.parse::<reqwest::header::HeaderValue>() else {
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
        eprintln!("<- {status} {path}");
    }
    let mut resp = HttpResponse::build(status);
    for (k, v) in res.headers().iter() {
        let name = k.as_str();
        // BodyStream 重新生成传输 framing；reqwest 未启用解压 feature，保留 content-encoding。
        if is_hop_by_hop(name)
            || name == "content-length"
            || is_connection_named(
                res.headers()
                    .get_all(reqwest::header::CONNECTION)
                    .iter()
                    .filter_map(|value| value.to_str().ok()),
                name,
            )
        {
            continue;
        }
        // 非法 header 值丢弃, 不阻断响应
        if let Ok(s) = v.to_str() {
            if let Ok(val) = s.parse::<header::HeaderValue>() {
                resp.append_header((name, val));
            }
        }
    }
    // 流式转发: SSE / chunked 响应按块透传, 不等整个 body
    Ok(resp.body(BodyStream::new(res.bytes_stream())))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cfg = load_cfg().map_err(std::io::Error::other)?;
    if cfg.debug {
        eprintln!("debug: on (request logs on stderr)");
    }
    if let Some(p) = cfg.patch.as_object() {
        if !p.is_empty() {
            println!("patch: {p:?}");
        }
    }
    if let Some(r) = cfg.rename.as_object() {
        if !r.is_empty() {
            println!("rename: {r:?}");
        }
    }

    let server_cfg = cfg.clone();
    let app = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(server_cfg.clone()))
            .app_data(web::PayloadConfig::new(16 << 20)) // ponytail: 长对话 JSON 常超默认 256KB; 16MB 足够, 再大按需调
            .service(web::resource("/{path:.*}").route(web::to(proxy)))
    })
    .bind(("0.0.0.0", cfg.port))?; // 0.0.0.0: 供 OrbStack 容器内 pentagi 经 orb.local (VM 网关) 访问

    println!(
        "listening on http://127.0.0.1:{}  ->  {}",
        cfg.port, cfg.upstream
    );
    app.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg(upstream: String) -> Cfg {
        Cfg {
            port: 8080,
            upstream,
            patch: serde_json::json!({"reasoning_effort": "high"}),
            rename: serde_json::json!({"max_tokens": "max_completion_tokens"}),
            debug: false,
            api_key: Some("test-key".to_string()),
            client: reqwest::Client::new(),
        }
    }

    fn raw_header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
        head.lines().skip(1).find_map(|line| {
            let (candidate, value) = line.split_once(':')?;
            candidate.eq_ignore_ascii_case(name).then(|| value.trim())
        })
    }

    fn spawn_asserting_upstream() -> (String, std::thread::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let thread = std::thread::spawn(move || {
            use std::io::{Read, Write};

            let (mut stream, _) = listener.accept().unwrap();
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

            assert!(head.starts_with("POST /v1/chat/completions?probe=1 HTTP/1.1\r\n"));
            assert_eq!(raw_header(&head, "authorization"), Some("Bearer test-key"));
            assert_eq!(raw_header(&head, "user-agent"), Some(BROWSER_UA));
            assert_eq!(raw_header(&head, "x-request-id"), Some("request-42"));
            assert_eq!(raw_header(&head, "connection"), None);
            assert_eq!(raw_header(&head, "x-client-hop"), None);
            assert_eq!(raw_header(&head, "te"), None);
            let body: Value =
                serde_json::from_slice(&request[body_start..body_start + content_length]).unwrap();
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
        let (upstream, upstream_thread) = spawn_asserting_upstream();

        let app = actix_web::test::init_service(
            App::new()
                .app_data(web::Data::new(test_cfg(upstream)))
                .service(web::resource("/{path:.*}").route(web::to(proxy))),
        )
        .await;
        let req = actix_web::test::TestRequest::post()
            .uri("/v1/chat/completions?probe=1")
            .insert_header((header::CONTENT_TYPE, "application/json"))
            .insert_header((header::CONNECTION, "x-client-hop"))
            .insert_header(("x-client-hop", "secret"))
            .insert_header((header::TE, "trailers"))
            .insert_header(("x-request-id", "request-42"))
            .set_payload(r#"{"model":"demo","max_tokens":128}"#)
            .to_request();
        let response = actix_web::test::call_service(&app, req).await;

        assert_eq!(response.status(), StatusCode::CREATED);
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

    #[test]
    fn rename_field() {
        let mut body = br#"{"model":"m","max_tokens":2048}"#.to_vec();
        apply_rename(
            &mut body,
            &serde_json::json!({"max_tokens": "max_completion_tokens"}),
        );
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("max_tokens").is_none());
        assert_eq!(v["max_completion_tokens"], 2048);
    }

    #[test]
    fn rename_then_patch() {
        let mut body = br#"{"max_tokens":100}"#.to_vec();
        apply_rename(
            &mut body,
            &serde_json::json!({"max_tokens": "max_completion_tokens"}),
        );
        // rename 后字段名已变, patch 需操作新名字
        apply_patch(
            &mut body,
            &serde_json::json!({"max_completion_tokens": 8192}),
        );
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["max_completion_tokens"], 8192);
        assert!(v.get("max_tokens").is_none());
    }

    #[test]
    fn patch_merge_and_remove() {
        let mut body = br#"{"model":"old","stream":true}"#.to_vec();
        apply_patch(
            &mut body,
            &serde_json::json!({"model": "new", "extra": 1, "stream": null}),
        );
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["model"], "new");
        assert_eq!(v["extra"], 1);
        assert!(v.get("stream").is_none());
    }

    #[test]
    fn patch_non_json_noop() {
        let mut body = b"not json".to_vec();
        apply_patch(&mut body, &serde_json::json!({"model": "new"}));
        assert_eq!(body, b"not json");
    }

    #[test]
    fn patch_null_or_non_object_noop() {
        let mut body = br#"{"model":"old"}"#.to_vec();
        let mut body2 = body.clone();
        apply_patch(&mut body, &Value::Null);
        apply_patch(&mut body2, &serde_json::json!([1, 2]));
        assert_eq!(body, body2);
    }
}
