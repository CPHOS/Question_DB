//! Shared low-level filesystem helpers.

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};

pub(crate) fn canonical_or_original(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string()
}

pub(crate) fn normalize_bundle_description(field: &str, value: &str) -> Result<String> {
    let normalized = value.trim().to_string();
    validate_bundle_description(field, &normalized)?;
    Ok(normalized)
}

pub(crate) fn normalize_optional_bundle_description(
    field: &str,
    value: Option<String>,
) -> Result<String> {
    let Some(text) = value else {
        bail!("{field} must not be null");
    };
    normalize_bundle_description(field, &text)
}

pub(crate) fn bundle_directory_name(description: &str, id: &str) -> String {
    let suffix = id
        .chars()
        .filter(|ch| *ch != '-')
        .take(6)
        .collect::<String>();
    format!("{description}_{suffix}")
}

fn validate_bundle_description(field: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    if value == "." || value == ".." {
        bail!("{field} must not be '.' or '..'");
    }
    if value.ends_with('.') {
        bail!("{field} must not end with '.'");
    }
    if value.chars().count() > 80 {
        bail!("{field} must be at most 80 characters");
    }

    for ch in value.chars() {
        if ch.is_control() {
            bail!("{field} must not contain control characters");
        }
        if matches!(ch, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
            bail!("{field} contains an invalid filename character: {ch}");
        }
    }

    Ok(())
}

/// Validate that a string can be parsed as an ISO 8601 timestamp.
/// Accepts `YYYY-MM-DD` (treated as midnight UTC) or full RFC 3339 strings.
pub(crate) fn validate_timestamp_param(field: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{field} must not be empty");
    }
    // Try YYYY-MM-DD first
    if chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").is_ok() {
        return Ok(());
    }
    // Try full RFC 3339 / ISO 8601
    if chrono::DateTime::parse_from_rfc3339(trimmed).is_ok() {
        return Ok(());
    }
    // Try without timezone (naive datetime)
    if chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S").is_ok()
        || chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f").is_ok()
    {
        return Ok(());
    }
    bail!("{field} must be a valid ISO 8601 date or datetime (e.g. 2026-01-01 or 2026-01-01T00:00:00Z)");
}

/// Escape SQL ILIKE special characters (`%`, `_`, `\`) so they match literally.
pub(crate) fn escape_ilike(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '%' | '_' | '\\' => {
                result.push('\\');
                result.push(ch);
            }
            _ => result.push(ch),
        }
    }
    result
}

/// Resolve an optional user-provided export path to an absolute path inside
/// `export_dir`.  Absolute paths and `..` components are rejected.
pub(crate) fn resolve_export_path(
    user_path: Option<&str>,
    default: PathBuf,
    export_dir: &Path,
) -> Result<PathBuf> {
    let relative = match user_path {
        Some(p) => {
            let candidate = Path::new(p);
            if candidate.is_absolute() {
                bail!("output_path must be a relative path");
            }
            for component in candidate.components() {
                if matches!(component, std::path::Component::ParentDir) {
                    bail!("output_path must not contain '..'");
                }
            }
            PathBuf::from(p)
        }
        None => default,
    };
    Ok(export_dir.join(relative))
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        bundle_directory_name, escape_ilike, normalize_bundle_description, resolve_export_path,
    };

    #[test]
    fn normalize_bundle_description_accepts_chinese() {
        let normalized =
            normalize_bundle_description("description", "  热学 决赛卷 A  ").expect("valid");
        assert_eq!(normalized, "热学 决赛卷 A");
    }

    #[test]
    fn normalize_bundle_description_rejects_invalid_filename_chars() {
        let err = normalize_bundle_description("description", "bad/name").expect_err("should fail");
        assert!(err.to_string().contains("invalid filename character"));
    }

    #[test]
    fn bundle_directory_name_appends_id_suffix() {
        let directory = bundle_directory_name("热学决赛卷", "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(directory, "热学决赛卷_550e84");
    }

    #[test]
    fn escape_ilike_escapes_special_characters() {
        assert_eq!(escape_ilike("100%"), r"100\%");
        assert_eq!(escape_ilike("a_b"), r"a\_b");
        assert_eq!(escape_ilike(r"c\d"), r"c\\d");
        assert_eq!(escape_ilike("hello"), "hello");
        assert_eq!(escape_ilike("%_\\"), r"\%\_\\");
    }

    #[test]
    fn resolve_export_path_joins_relative() {
        let dir = Path::new("/data/exports");
        let result = resolve_export_path(Some("out.jsonl"), PathBuf::from("default.jsonl"), dir)
            .expect("should succeed");
        assert_eq!(result, PathBuf::from("/data/exports/out.jsonl"));
    }

    #[test]
    fn resolve_export_path_uses_default_when_none() {
        let dir = Path::new("/data/exports");
        let result =
            resolve_export_path(None, PathBuf::from("default.jsonl"), dir).expect("should succeed");
        assert_eq!(result, PathBuf::from("/data/exports/default.jsonl"));
    }

    #[test]
    fn resolve_export_path_rejects_absolute_path() {
        let dir = Path::new("/data/exports");
        let err = resolve_export_path(Some("/etc/passwd"), PathBuf::from("d.jsonl"), dir)
            .expect_err("should fail");
        assert!(err.to_string().contains("relative path"));
    }

    #[test]
    fn resolve_export_path_rejects_parent_dir() {
        let dir = Path::new("/data/exports");
        let err = resolve_export_path(Some("../secret.json"), PathBuf::from("d.jsonl"), dir)
            .expect_err("should fail");
        assert!(err.to_string().contains(".."));
    }
}
