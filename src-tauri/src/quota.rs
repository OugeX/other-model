use crate::{
    models::{BalanceAuthMode, ProviderConfig, QuotaResult},
    storage::Storage,
};
use reqwest::{header::SET_COOKIE, Client, Url};
use serde_json::{json, Value};

pub async fn get_quota(storage: Storage, provider_id: String) -> QuotaResult {
    let cfg = storage.config().await;
    let Some(provider) = cfg.providers.into_iter().find(|p| p.id == provider_id) else {
        return QuotaResult {
            provider_id,
            ok: false,
            error: Some("provider not found".to_string()),
            ..Default::default()
        };
    };
    query_quota(provider).await
}

async fn query_quota(provider: ProviderConfig) -> QuotaResult {
    match provider.effective_balance_auth().mode {
        BalanceAuthMode::Disabled => QuotaResult {
            provider_id: provider.id,
            ok: true,
            health_hint: Some("未配置余额查询方式".to_string()),
            ..Default::default()
        },
        BalanceAuthMode::QuotaApi => query_quota_api(provider).await,
        BalanceAuthMode::NewapiLogin => query_newapi_balance(provider).await,
        BalanceAuthMode::Sub2apiLogin => query_sub2api_balance(provider).await,
    }
}

async fn query_quota_api(provider: ProviderConfig) -> QuotaResult {
    let Some(quota) = provider.quota.clone() else {
        return quota_failure(&provider.id, None, "quota_api 模式缺少 quota 配置", None);
    };
    if quota.url.trim().is_empty() {
        return quota_failure(&provider.id, None, "quota URL is empty", None);
    }
    let client = match client_for_provider(&provider, false) {
        Ok(client) => client,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };

    let mut req = if quota.method.eq_ignore_ascii_case("POST") {
        client.post(&quota.url)
    } else {
        client.get(&quota.url)
    };
    req = req.bearer_auth(&provider.api_key);
    for (k, v) in quota.headers {
        req = req.header(k, v);
    }

    match req.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let raw = read_response_body(resp).await;
            let balance = quota
                .balance_json_path
                .as_deref()
                .and_then(|path| json_path(&raw, path))
                .map(value_to_string);
            QuotaResult {
                provider_id: provider.id,
                ok: (200..300).contains(&status),
                balance,
                status: Some(status),
                raw: Some(raw),
                error: if (200..300).contains(&status) {
                    None
                } else {
                    Some(format!("quota endpoint returned {status}"))
                },
                ..Default::default()
            }
        }
        Err(err) => quota_failure(&provider.id, None, err.to_string(), None),
    }
}

async fn query_newapi_balance(provider: ProviderConfig) -> QuotaResult {
    let auth = provider.effective_balance_auth();
    let username = match required_credential(auth.username, "用户名") {
        Ok(value) => value,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };
    let password = match required_credential(auth.password, "密码") {
        Ok(value) => value,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };
    let root = match normalize_newapi_root(&provider.base_url) {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };
    let login_url = match root.join("api/user/login") {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err.to_string(), None),
    };
    let self_url = match root.join("api/user/self") {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err.to_string(), None),
    };
    let status_url = match root.join("api/status") {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err.to_string(), None),
    };
    let client = match client_for_provider(&provider, true) {
        Ok(client) => client,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };

    let login_resp = match client
        .post(login_url)
        .json(&json!({
            "username": username,
            "password": password,
        }))
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => return quota_failure(&provider.id, None, format!("登录请求失败: {err}"), None),
    };
    let login_status = login_resp.status().as_u16();
    let has_cookie = login_resp
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .next()
        .is_some();
    let login_raw = read_response_body(login_resp).await;

    if requires_2fa(&login_raw) {
        return quota_failure(
            &provider.id,
            Some(login_status),
            "newapi 登录返回需要 2FA，当前适配器不支持",
            Some(json!({ "login": login_raw })),
        );
    }

    if login_status == 401
        || response_success(&login_raw) == Some(false)
        || !(200..300).contains(&login_status)
    {
        let message = upstream_login_error_message(login_status, &login_raw)
            .unwrap_or_else(|| format!("HTTP {login_status}"));
        return quota_failure(
            &provider.id,
            Some(login_status),
            format!("登录失败: {message}"),
            Some(json!({ "login": login_raw })),
        );
    }

    if !has_cookie {
        return quota_failure(
            &provider.id,
            Some(login_status),
            "登录成功但未收到会话 cookie",
            Some(json!({ "login": login_raw })),
        );
    }

    let login_data = response_data(&login_raw);
    let Some(user_id) = login_data.get("id") else {
        return quota_failure(
            &provider.id,
            Some(login_status),
            "登录成功但未返回用户 ID，无法请求当前用户信息",
            Some(json!({ "login": login_raw })),
        );
    };
    let user_id = value_to_string(user_id);

    let self_resp = match client
        .get(self_url)
        .header("New-Api-User", &user_id)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            return quota_failure(
                &provider.id,
                None,
                format!("获取当前用户信息失败: {err}"),
                Some(json!({ "login": login_raw })),
            )
        }
    };
    let self_status = self_resp.status().as_u16();
    let self_raw = read_response_body(self_resp).await;

    if !(200..300).contains(&self_status) || response_success(&self_raw) == Some(false) {
        let message = response_message(&self_raw).unwrap_or_else(|| format!("HTTP {self_status}"));
        return quota_failure(
            &provider.id,
            Some(self_status),
            format!("获取当前用户信息失败: {message}"),
            Some(json!({
                "login": login_raw,
                "self": self_raw,
            })),
        );
    }

    let self_data = response_data(&self_raw);
    let Some(quota) = self_data.get("quota") else {
        return quota_failure(
            &provider.id,
            Some(self_status),
            "当前用户信息响应缺少 quota 字段",
            Some(json!({
                "login": login_raw,
                "self": self_raw,
            })),
        );
    };
    let status_raw = fetch_newapi_status(&client, status_url).await;
    let combined_raw = json!({
        "login": login_raw,
        "self": self_raw,
        "status": status_raw,
    });
    let display_config = newapi_display_config(&combined_raw["status"]);
    let balance = display_config
        .as_ref()
        .map(|cfg| render_newapi_quota(quota, cfg))
        .unwrap_or_else(|| format!("quota={}", value_to_string(quota)));
    let health_hint = self_data.get("used_quota").map(|used| {
        display_config
            .as_ref()
            .map(|cfg| format!("历史消耗={}", render_newapi_quota(used, cfg)))
            .unwrap_or_else(|| format!("used_quota={}", value_to_string(used)))
    });

    QuotaResult {
        provider_id: provider.id,
        ok: true,
        balance: Some(balance),
        health_hint,
        status: Some(self_status),
        raw: Some(combined_raw),
        ..Default::default()
    }
}

async fn fetch_newapi_status(client: &Client, status_url: Url) -> Value {
    let Ok(resp) = client.get(status_url).send().await else {
        return Value::Null;
    };
    if !resp.status().is_success() {
        return Value::Null;
    }
    read_response_body(resp).await
}

async fn query_sub2api_balance(provider: ProviderConfig) -> QuotaResult {
    let auth = provider.effective_balance_auth();
    let username = match required_credential(auth.username, "邮箱/账号") {
        Ok(value) => value,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };
    let password = match required_credential(auth.password, "密码") {
        Ok(value) => value,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };
    let api_base = match normalize_sub2api_api_base(&provider.base_url) {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };
    let login_url = match api_base.join("auth/login") {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err.to_string(), None),
    };
    let me_url = match api_base.join("auth/me") {
        Ok(url) => url,
        Err(err) => return quota_failure(&provider.id, None, err.to_string(), None),
    };
    let client = match client_for_provider(&provider, false) {
        Ok(client) => client,
        Err(err) => return quota_failure(&provider.id, None, err, None),
    };

    let login_resp = match client
        .post(login_url)
        .json(&json!({
            "email": username,
            "password": password,
        }))
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(err) => return quota_failure(&provider.id, None, format!("登录请求失败: {err}"), None),
    };
    let login_status = login_resp.status().as_u16();
    let login_raw = read_response_body(login_resp).await;

    if requires_2fa(&login_raw) {
        return quota_failure(
            &provider.id,
            Some(login_status),
            "sub2api 登录返回需要 2FA，当前适配器不支持",
            Some(json!({ "login": login_raw })),
        );
    }

    if !(200..300).contains(&login_status) || response_success(&login_raw) == Some(false) {
        if login_status == 400
            && !username.contains('@')
            && response_message(&login_raw)
                .unwrap_or_default()
                .contains("LoginRequest.Email")
        {
            return quota_failure(
                &provider.id,
                Some(login_status),
                "sub2api 官方登录接口要求填写完整登录邮箱，不能只填站内账号/昵称",
                Some(json!({ "login": login_raw })),
            );
        }
        let message = upstream_login_error_message(login_status, &login_raw)
            .unwrap_or_else(|| format!("HTTP {login_status}"));
        return quota_failure(
            &provider.id,
            Some(login_status),
            format!("登录失败: {message}"),
            Some(json!({ "login": login_raw })),
        );
    }

    let login_data = response_data(&login_raw);
    let Some(access_token) = login_data.get("access_token").and_then(Value::as_str) else {
        return quota_failure(
            &provider.id,
            Some(login_status),
            "登录成功但未返回 access_token",
            Some(json!({ "login": login_raw })),
        );
    };

    let me_resp = match client.get(me_url).bearer_auth(access_token).send().await {
        Ok(resp) => resp,
        Err(err) => {
            return quota_failure(
                &provider.id,
                None,
                format!("获取当前用户信息失败: {err}"),
                Some(json!({ "login": login_raw })),
            )
        }
    };
    let me_status = me_resp.status().as_u16();
    let me_raw = read_response_body(me_resp).await;
    let combined_raw = json!({
        "login": login_raw,
        "self": me_raw,
    });

    if !(200..300).contains(&me_status) || response_success(&me_raw) == Some(false) {
        let message = response_message(&me_raw).unwrap_or_else(|| format!("HTTP {me_status}"));
        return quota_failure(
            &provider.id,
            Some(me_status),
            format!("获取当前用户信息失败: {message}"),
            Some(combined_raw),
        );
    }

    let me_data = response_data(&me_raw);
    let Some(balance) = me_data.get("balance") else {
        return quota_failure(
            &provider.id,
            Some(me_status),
            "当前用户信息响应缺少 balance 字段",
            Some(combined_raw),
        );
    };
    let balance_display = value_to_f64(Some(balance))
        .map(|value| format!("${value:.2}"))
        .unwrap_or_else(|| value_to_string(balance));

    QuotaResult {
        provider_id: provider.id,
        ok: true,
        balance: Some(balance_display),
        status: Some(me_status),
        raw: Some(combined_raw),
        ..Default::default()
    }
}

fn client_for_provider(provider: &ProviderConfig, cookie_store: bool) -> Result<Client, String> {
    let mut builder = Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(provider.timeout_secs.max(1)));
    if cookie_store {
        builder = builder.cookie_store(true);
    }
    builder.build().map_err(|err| err.to_string())
}

fn required_credential(value: Option<String>, field_name: &str) -> Result<String, String> {
    let value = value.unwrap_or_default().trim().to_string();
    if value.is_empty() {
        Err(format!("{}不能为空", field_name))
    } else {
        Ok(value)
    }
}

fn normalize_newapi_root(base_url: &str) -> Result<Url, String> {
    let mut url = Url::parse(base_url).map_err(|err| format!("base URL 无效: {err}"))?;
    url.set_query(None);
    url.set_fragment(None);
    let path = url.path().trim_end_matches('/');
    let normalized = if path.is_empty() || path == "/" {
        "/".to_string()
    } else if path.ends_with("/v1") {
        let trimmed = path.trim_end_matches("/v1").trim_end_matches('/');
        if trimmed.is_empty() {
            "/".to_string()
        } else {
            format!("{trimmed}/")
        }
    } else {
        format!("{}/", path)
    };
    url.set_path(&normalized);
    Ok(url)
}

fn normalize_sub2api_api_base(base_url: &str) -> Result<Url, String> {
    let mut url = Url::parse(base_url).map_err(|err| format!("base URL 无效: {err}"))?;
    url.set_query(None);
    url.set_fragment(None);
    let path = url.path().trim_end_matches('/');
    let normalized = if path.is_empty() || path == "/" {
        "/api/v1/".to_string()
    } else if path == "/v1" {
        "/api/v1/".to_string()
    } else if let Some(index) = path.find("/api/v1") {
        format!("{}/", &path[..index + "/api/v1".len()])
    } else {
        format!("{}/api/v1/", path)
    };
    url.set_path(&normalized);
    Ok(url)
}

fn quota_failure(
    provider_id: &str,
    status: Option<u16>,
    error: impl Into<String>,
    raw: Option<Value>,
) -> QuotaResult {
    QuotaResult {
        provider_id: provider_id.to_string(),
        ok: false,
        status,
        error: Some(error.into()),
        raw,
        ..Default::default()
    }
}

async fn read_response_body(resp: reqwest::Response) -> Value {
    match resp.bytes().await {
        Ok(bytes) if bytes.is_empty() => Value::Null,
        Ok(bytes) => serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string())),
        Err(err) => Value::String(format!("<read body failed: {err}>")),
    }
}

fn response_success(value: &Value) -> Option<bool> {
    if let Some(success) = value.get("success").and_then(Value::as_bool) {
        return Some(success);
    }
    if let Some(code) = value.get("code").and_then(Value::as_i64) {
        return Some(code == 0);
    }
    None
}

fn response_data(value: &Value) -> &Value {
    value.get("data").unwrap_or(value)
}

fn response_message(value: &Value) -> Option<String> {
    if let Some(text) = value.as_str() {
        let text = text.trim();
        if !text.is_empty() {
            return Some(text.to_string());
        }
    }
    if let Some(message) = value.get("message").and_then(Value::as_str) {
        let message = message.trim();
        if !message.is_empty() {
            return Some(message.to_string());
        }
    }
    if let Some(reason) = value.get("reason").and_then(Value::as_str) {
        let reason = reason.trim();
        if !reason.is_empty() {
            return Some(reason.to_string());
        }
    }
    if let Some(error) = value.get("error") {
        if let Some(text) = error.as_str() {
            let text = text.trim();
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
        if let Some(text) = error.get("message").and_then(Value::as_str) {
            let text = text.trim();
            if !text.is_empty() {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn upstream_login_error_message(status: u16, value: &Value) -> Option<String> {
    let message = response_message(value);
    if status == 403
        && message
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .contains("1010")
    {
        return Some("目标站点启用了 Cloudflare/WAF 1010，当前服务器 IP 被拦截".to_string());
    }
    message
}

fn requires_2fa(value: &Value) -> bool {
    response_data(value)
        .get("require_2fa")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || response_data(value)
            .get("requires_2fa")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || value
            .get("require_2fa")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        || value
            .get("requires_2fa")
            .and_then(Value::as_bool)
            .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct NewapiDisplayConfig {
    quota_per_unit: f64,
    quota_display_type: String,
    usd_exchange_rate: f64,
    custom_currency_symbol: String,
    custom_currency_exchange_rate: f64,
}

fn newapi_display_config(status_raw: &Value) -> Option<NewapiDisplayConfig> {
    let status = response_data(status_raw);
    if status.is_null() {
        return None;
    }
    let quota_display_type = status
        .get("quota_display_type")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_uppercase())
        .or_else(|| {
            status
                .get("display_in_currency")
                .and_then(Value::as_bool)
                .map(|enabled| {
                    if enabled {
                        "USD".to_string()
                    } else {
                        "TOKENS".to_string()
                    }
                })
        })
        .unwrap_or_else(|| "USD".to_string());
    Some(NewapiDisplayConfig {
        quota_per_unit: value_to_f64(status.get("quota_per_unit")).unwrap_or(500_000.0),
        quota_display_type,
        usd_exchange_rate: value_to_f64(status.get("usd_exchange_rate")).unwrap_or(1.0),
        custom_currency_symbol: status
            .get("custom_currency_symbol")
            .and_then(Value::as_str)
            .unwrap_or("¤")
            .to_string(),
        custom_currency_exchange_rate: value_to_f64(status.get("custom_currency_exchange_rate"))
            .unwrap_or(1.0),
    })
}

fn render_newapi_quota(quota: &Value, cfg: &NewapiDisplayConfig) -> String {
    let Some(quota_value) = value_to_f64(Some(quota)) else {
        return value_to_string(quota);
    };
    if cfg.quota_display_type == "TOKENS" {
        return render_newapi_number(quota_value, quota);
    }

    let quota_per_unit = if cfg.quota_per_unit > 0.0 {
        cfg.quota_per_unit
    } else {
        500_000.0
    };
    let result_usd = quota_value / quota_per_unit;
    let (symbol, value) = match cfg.quota_display_type.as_str() {
        "CNY" => ("¥".to_string(), result_usd * cfg.usd_exchange_rate.max(0.0)),
        "CUSTOM" => (
            if cfg.custom_currency_symbol.trim().is_empty() {
                "¤".to_string()
            } else {
                cfg.custom_currency_symbol.clone()
            },
            result_usd * cfg.custom_currency_exchange_rate.max(0.0),
        ),
        _ => ("$".to_string(), result_usd),
    };
    let fixed = format!("{value:.2}");
    if fixed.parse::<f64>().unwrap_or_default() == 0.0 && quota_value > 0.0 && value > 0.0 {
        return format!("{symbol}{:.2}", 0.01);
    }
    format!("{symbol}{fixed}")
}

fn render_newapi_number(num: f64, original: &Value) -> String {
    if num >= 1_000_000_000.0 {
        format!("{:.1}B", num / 1_000_000_000.0)
    } else if num >= 1_000_000.0 {
        format!("{:.1}M", num / 1_000_000.0)
    } else if num >= 10_000.0 {
        format!("{:.1}k", num / 1_000.0)
    } else {
        value_to_string(original)
    }
}

fn value_to_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn json_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path
        .trim_start_matches('$')
        .trim_start_matches('.')
        .split('.')
    {
        if segment.is_empty() {
            continue;
        }
        current = current.get(segment)?;
    }
    Some(current)
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{BalanceAuthConfig, QuotaConfig};
    use std::collections::BTreeMap;
    use wiremock::{
        matchers::{body_json, header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn provider(base_url: String) -> ProviderConfig {
        ProviderConfig {
            id: "p1".to_string(),
            name: "Provider 1".to_string(),
            base_url,
            api_key: "api-key-1".to_string(),
            enabled: true,
            timeout_secs: 5,
            headers: BTreeMap::new(),
            query: BTreeMap::new(),
            quota: None,
            balance_auth: None,
        }
    }

    #[tokio::test]
    async fn quota_api_mode_remains_compatible_with_legacy_quota_config() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/quota"))
            .and(header("authorization", "Bearer api-key-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": { "balance": 321 }
            })))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.quota = Some(QuotaConfig {
            url: format!("{}/quota", server.uri()),
            method: "GET".to_string(),
            headers: BTreeMap::new(),
            balance_json_path: Some("$.data.balance".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(result.ok);
        assert_eq!(result.balance.as_deref(), Some("321"));
        assert_eq!(result.status, Some(200));
    }

    #[tokio::test]
    async fn disabled_mode_returns_hint() {
        let mut provider = provider("https://example.com/v1".to_string());
        provider.balance_auth = Some(BalanceAuthConfig::default());

        let result = query_quota(provider).await;
        assert!(result.ok);
        assert_eq!(result.health_hint.as_deref(), Some("未配置余额查询方式"));
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn newapi_login_success_supports_root_and_v1_base_urls() {
        for base_url in newapi_base_urls().await {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/user/login"))
                .and(body_json(json!({
                    "username": "alice",
                    "password": "secret"
                })))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("set-cookie", "session=abc; Path=/; HttpOnly")
                        .set_body_json(json!({
                            "success": true,
                            "message": "",
                            "data": { "id": 1, "username": "alice" }
                        })),
                )
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/user/self"))
                .and(header("new-api-user", "1"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "success": true,
                    "message": "",
                    "data": {
                        "quota": 15280940,
                        "used_quota": 39929060
                    }
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/status"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "success": true,
                    "message": "",
                    "data": {
                        "quota_per_unit": 500000,
                        "quota_display_type": "USD",
                        "display_in_currency": true
                    }
                })))
                .mount(&server)
                .await;

            let mut provider = provider(base_url(server.uri()));
            provider.balance_auth = Some(BalanceAuthConfig {
                mode: BalanceAuthMode::NewapiLogin,
                username: Some("alice".to_string()),
                password: Some("secret".to_string()),
            });

            let result = query_quota(provider).await;
            assert!(result.ok, "result error: {:?}", result.error);
            assert_eq!(result.balance.as_deref(), Some("$30.56"));
            assert_eq!(result.health_hint.as_deref(), Some("历史消耗=$79.86"));
            assert_eq!(result.status, Some(200));
        }
    }

    #[tokio::test]
    async fn newapi_login_business_failure_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/user/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": false,
                "message": "invalid credentials"
            })))
            .mount(&server)
            .await;

        let mut provider = provider(format!("{}/v1", server.uri()));
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::NewapiLogin,
            username: Some("alice".to_string()),
            password: Some("bad".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("登录失败"));
    }

    #[tokio::test]
    async fn newapi_login_2fa_is_not_supported() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/user/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": true,
                "message": "",
                "data": { "require_2fa": true }
            })))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::NewapiLogin,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result.error.as_deref().unwrap_or_default().contains("2FA"));
    }

    #[tokio::test]
    async fn newapi_login_requires_user_id_for_self_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/user/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("set-cookie", "session=abc; Path=/; HttpOnly")
                    .set_body_json(json!({
                        "success": true,
                        "message": "",
                        "data": { "username": "alice" }
                    })),
            )
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::NewapiLogin,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("用户 ID"));
    }

    #[tokio::test]
    async fn newapi_login_falls_back_to_raw_quota_when_status_is_unavailable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/user/login"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("set-cookie", "session=abc; Path=/; HttpOnly")
                    .set_body_json(json!({
                        "success": true,
                        "message": "",
                        "data": { "id": 1, "username": "alice" }
                    })),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/user/self"))
            .and(header("new-api-user", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "success": true,
                "message": "",
                "data": {
                    "quota": 123456,
                    "used_quota": 7890
                }
            })))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::NewapiLogin,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(result.ok);
        assert_eq!(result.balance.as_deref(), Some("quota=123456"));
        assert_eq!(result.health_hint.as_deref(), Some("used_quota=7890"));
    }

    #[tokio::test]
    async fn sub2api_login_success_supports_root_and_api_v1_base_urls() {
        for base_url in sub2api_base_urls().await {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/v1/auth/login"))
                .and(body_json(json!({
                    "email": "alice@example.com",
                    "password": "secret"
                })))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "code": 0,
                    "message": "success",
                    "data": {
                        "access_token": "token-123",
                        "token_type": "Bearer"
                    }
                })))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v1/auth/me"))
                .and(header("authorization", "Bearer token-123"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "code": 0,
                    "message": "success",
                    "data": {
                        "id": 1,
                        "balance": 88.5
                    }
                })))
                .mount(&server)
                .await;

            let mut provider = provider(base_url(server.uri()));
            provider.balance_auth = Some(BalanceAuthConfig {
                mode: BalanceAuthMode::Sub2apiLogin,
                username: Some("alice@example.com".to_string()),
                password: Some("secret".to_string()),
            });

            let result = query_quota(provider).await;
            assert!(result.ok, "result error: {:?}", result.error);
            assert_eq!(result.balance.as_deref(), Some("$88.50"));
            assert_eq!(result.status, Some(200));
        }
    }

    #[tokio::test]
    async fn sub2api_login_failure_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/auth/login"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({
                "code": 401,
                "message": "invalid credentials"
            })))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::Sub2apiLogin,
            username: Some("alice@example.com".to_string()),
            password: Some("bad".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("登录失败"));
    }

    #[tokio::test]
    async fn sub2api_login_requires_full_email_when_server_validates_email() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/auth/login"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({
                "code": 400,
                "message": "Invalid request: Key: 'LoginRequest.Email' Error:Field validation for 'Email' failed on the 'email' tag"
            })))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::Sub2apiLogin,
            username: Some("810066828".to_string()),
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("完整登录邮箱"));
    }

    #[tokio::test]
    async fn upstream_waf_1010_error_is_made_explicit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/auth/login"))
            .respond_with(ResponseTemplate::new(403).set_body_string("error code: 1010"))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::Sub2apiLogin,
            username: Some("alice@example.com".to_string()),
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("Cloudflare/WAF 1010"));
    }

    #[tokio::test]
    async fn sub2api_login_2fa_is_not_supported() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/v1/auth/login"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "code": 0,
                "message": "success",
                "data": {
                    "requires_2fa": true,
                    "temp_token": "tmp-1"
                }
            })))
            .mount(&server)
            .await;

        let mut provider = provider(server.uri());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::Sub2apiLogin,
            username: Some("alice@example.com".to_string()),
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result.error.as_deref().unwrap_or_default().contains("2FA"));
    }

    #[tokio::test]
    async fn login_mode_requires_username_and_password() {
        let mut provider = provider("https://example.com".to_string());
        provider.balance_auth = Some(BalanceAuthConfig {
            mode: BalanceAuthMode::Sub2apiLogin,
            username: None,
            password: Some("secret".to_string()),
        });

        let result = query_quota(provider).await;
        assert!(!result.ok);
        assert!(result
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("邮箱/账号"));
    }

    async fn newapi_base_urls() -> Vec<Box<dyn Fn(String) -> String>> {
        vec![Box::new(|root| root), Box::new(|root| format!("{root}/v1"))]
    }

    async fn sub2api_base_urls() -> Vec<Box<dyn Fn(String) -> String>> {
        vec![
            Box::new(|root| root),
            Box::new(|root| format!("{root}/v1")),
            Box::new(|root| format!("{root}/api/v1")),
        ]
    }
}
