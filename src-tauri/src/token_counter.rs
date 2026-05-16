use anyhow::Result;
use serde_json::Value;
use tiktoken_rs::{cl100k_base, o200k_base, CoreBPE};

pub fn estimate_request_tokens(model: Option<&str>, body: &[u8]) -> Result<i64> {
    let value: Value = serde_json::from_slice(body)?;
    estimate_value_tokens(model, &value)
}

pub fn estimate_value_tokens(model: Option<&str>, value: &Value) -> Result<i64> {
    let bpe = bpe_for_model(model)?;
    Ok(count_value_tokens(&bpe, value) as i64)
}

fn bpe_for_model(model: Option<&str>) -> Result<CoreBPE> {
    let model = model.unwrap_or_default().trim().to_ascii_lowercase();
    if model.starts_with("gpt-5")
        || model.starts_with("gpt-4.1")
        || model.starts_with("gpt-4o")
        || model.starts_with("o1")
        || model.starts_with("o3")
        || model.starts_with("o4")
    {
        return Ok(o200k_base()?);
    }
    Ok(cl100k_base()?)
}

fn count_value_tokens(bpe: &CoreBPE, value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(v) => count_text(bpe, if *v { "true" } else { "false" }),
        Value::Number(v) => count_text(bpe, &v.to_string()),
        Value::String(v) => count_text(bpe, v),
        Value::Array(items) => items.iter().map(|item| count_value_tokens(bpe, item)).sum(),
        Value::Object(map) => {
            let mut total = 0usize;
            for (key, item) in map {
                total += count_text(bpe, key);
                if matches!(
                    key.as_str(),
                    "instructions"
                        | "input"
                        | "messages"
                        | "tools"
                        | "reasoning"
                        | "summary"
                        | "content"
                        | "text"
                        | "prompt"
                ) {
                    total += count_value_tokens(bpe, item);
                } else if item.is_string() {
                    total += count_value_tokens(bpe, item);
                } else if item.is_array() || item.is_object() {
                    total += count_value_tokens(bpe, item);
                }
            }
            total
        }
    }
}

fn count_text(bpe: &CoreBPE, text: &str) -> usize {
    const APPROX_THRESHOLD_BYTES: usize = 64 * 1024;
    if text.len() > APPROX_THRESHOLD_BYTES {
        return (text.len() / 4).max(1);
    }
    bpe.encode_with_special_tokens(text).len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn estimates_response_request_tokens() {
        let value = json!({
            "model": "gpt-5.5",
            "instructions": "You are helpful.",
            "input": [{"role": "user", "content": [{"type": "input_text", "text": "hello world"}]}],
            "tools": [{"type": "web_search"}],
        });
        let count = estimate_value_tokens(Some("gpt-5.5"), &value).unwrap();
        assert!(count > 0);
    }
}
