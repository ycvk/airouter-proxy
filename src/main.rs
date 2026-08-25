use actix_web::body::BodyStream;
use actix_web::http::{header, StatusCode};
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use serde_json::Value;

// hop-by-hop + content-length + host: body 被 patch 后长度变化需重算; host 是客户端地址, 上游按 Host 路由, 必须让 reqwest 按上游 URL 重设
const SKIP_HEADERS: [&str; 7] = ["connection", "keep-alive", "transfer-encoding", "upgrade", "content-length", "host", "accept-encoding"]; // accept-encoding: 上游压缩会让 reqwest 解压后头体错配, 一律明文
const HOP_BY_HOP: [&str; 4] = ["connection", "keep-alive", "transfer-encoding", "upgrade"];
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

/// 配置: cwd 下的 config.json, 环境变量 PORT/UPSTREAM/API_KEY 可覆盖。
fn load_cfg() -> Cfg {
    let mut port: u16 = 8080;
    let mut upstream = "https://lenca.agentthread.ai".to_string();
    let mut patch: Value = Value::Null;
    let mut rename: Value = Value::Null;
    let mut api_key: Option<String> = None;
    let debug = std::env::args().any(|a| a == "--debug");

    if let Ok(s) = std::fs::read_to_string("config.json") {
        if let Ok(v) = serde_json::from_str::<Value>(&s) {
            if let Some(o) = v.as_object() {
                if let Some(p) = o.get("port").and_then(Value::as_u64) {
                    port = p as u16;
                }
                if let Some(u) = o.get("upstream").and_then(Value::as_str) {
                    upstream = u.to_string();
                }
                if let Some(p) = o.get("patch") {
                    patch = p.clone();
                }
                if let Some(r) = o.get("rename") {
                    rename = r.clone();
                }
                if let Some(k) = o.get("api_key").and_then(Value::as_str) {
                    api_key = Some(k.to_string());
                }
            }
        }
    }
    if let Ok(p) = std::env::var("PORT") {
        if let Ok(p) = p.parse() {
            port = p;
        }
    }
    if let Ok(u) = std::env::var("UPSTREAM") {
        upstream = u;
    }
    if let Ok(k) = std::env::var("API_KEY") {
        api_key = Some(k);
    }
    upstream = upstream.trim_end_matches('/').to_string();

    Cfg {
        port,
        upstream,
        patch,
        rename,
        debug,
        api_key,
        client: reqwest::Client::new(),
    }
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

fn is_skipped(name: &str) -> bool {
    SKIP_HEADERS.contains(&name)
}

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.contains(&name)
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
    for (k, v) in req.headers().iter() {
        let name = k.as_str();
        if is_skipped(name) {
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
        // content-encoding/content-length 随 reqwest 自动解压失效, 剥离避免客户端误判 (gzip: invalid header)
        if is_hop_by_hop(name) || name == "content-encoding" || name == "content-length" {
            continue;
        }
        if is_hop_by_hop(name) {
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
    let cfg = load_cfg();
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

    #[test]
    fn rename_field() {
        let mut body = br#"{"model":"m","max_tokens":2048}"#.to_vec();
        apply_rename(&mut body, &serde_json::json!({"max_tokens": "max_completion_tokens"}));
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v.get("max_tokens").is_none());
        assert_eq!(v["max_completion_tokens"], 2048);
    }

    #[test]
    fn rename_then_patch() {
        let mut body = br#"{"max_tokens":100}"#.to_vec();
        apply_rename(&mut body, &serde_json::json!({"max_tokens": "max_completion_tokens"}));
        // rename 后字段名已变, patch 需操作新名字
        apply_patch(&mut body, &serde_json::json!({"max_completion_tokens": 8192}));
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
