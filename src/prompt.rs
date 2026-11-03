/*
 * Part of xfer.
 * Copyright (C) Grzegorz Antoniak <xfer@anadoxin.org>
 *
 * SPDX-License-Identifier: GPL-3.0-only
 */

//! Single-pass prompt expansion. Values are never interpreted as templates or shell code.
use crate::daemon::SessionInfo;

fn basename(path: &str) -> &str {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() && path.starts_with('/') {
        "/"
    } else {
        trimmed.rsplit('/').next().unwrap_or("")
    }
}

pub(crate) fn format(template: &str, info: &SessionInfo, local: &str) -> (String, Vec<(u32, u32)>) {
    let proto = info.protocol.as_deref().unwrap_or("");
    let path = info.cwd.as_deref().unwrap_or("");
    let cwd = match (&info.protocol, &info.cwd) {
        (Some(proto), Some(path)) => format!("{proto}:{path}"),
        (Some(proto), None) => proto.clone(),
        _ => String::new(),
    };
    let location = if cwd.is_empty() {
        String::new()
    } else {
        format!("[{cwd}]")
    };
    let value = |name| match name {
        "cwd" => Some(cwd.as_str()),
        "cwdn" => Some(basename(path)),
        "user" => Some(info.user.as_str()),
        "host" => Some(info.host.as_str()),
        "proto" => Some(proto),
        "lcwd" => Some(local),
        "lcwdn" => Some(basename(local)),
        "location" => Some(location.as_str()),
        _ => None,
    };
    let mut output = String::new();
    let mut ranges = Vec::new();
    let mut rest = template;
    while let Some(index) = rest.find('$') {
        output.push_str(&rest[..index]);
        rest = &rest[index + 1..];
        if let Some(tail) = rest.strip_prefix('$') {
            output.push('$');
            rest = tail;
            continue;
        }
        let (name, consumed) = if rest.starts_with('{') {
            match rest.find('}') {
                Some(end) => (&rest[1..end], end + 1),
                None => {
                    output.push('$');
                    continue;
                }
            }
        } else {
            let end = rest
                .find(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
                .unwrap_or(rest.len());
            (&rest[..end], end)
        };
        if let Some(value) = value(name) {
            let start = output.len();
            output.push_str(value);
            let (offset, length) = match name {
                "cwd" if !path.is_empty() && !cwd.is_empty() => {
                    (value.len() - path.len(), path.len())
                }
                "location" if !path.is_empty() && !location.is_empty() => {
                    (value.len() - path.len() - 1, path.len())
                }
                "cwdn" | "lcwd" | "lcwdn" => (0, value.len()),
                _ => (0, 0),
            };
            if length != 0 {
                ranges.push((start + offset, start + offset + length));
            }
        } else {
            output.push('$');
            output.push_str(&rest[..consumed]);
        }
        rest = &rest[consumed..];
    }
    output.push_str(rest);
    // Escape controls while translating byte ranges to the escaped output.
    let mut clean = String::new();
    let mut clean_ranges = Vec::new();
    let mut range_index = 0;
    let mut active = None;
    for (index, ch) in output.char_indices() {
        if ranges
            .get(range_index)
            .is_some_and(|(start, _)| *start == index)
        {
            active = Some(clean.len());
        }
        if ch.is_control() {
            clean.extend(ch.escape_default());
        } else {
            clean.push(ch);
        }
        if ranges
            .get(range_index)
            .is_some_and(|(_, end)| *end == index + ch.len_utf8())
        {
            clean_ranges.push((active.take().unwrap() as u32, clean.len() as u32));
            range_index += 1;
        }
    }
    (clean, clean_ranges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_and_identity() {
        let info = SessionInfo {
            protocol: Some("sftp".into()),
            cwd: Some("/home/antek/".into()),
            user: "antek".into(),
            host: "example.org".into(),
            ..Default::default()
        };
        assert_eq!(
            format("xfer[$cwd]> ", &info, "/tmp/work").0,
            "xfer[sftp:/home/antek/]> "
        );
        assert_eq!(
            format(
                "${user}@$host $proto $cwdn $lcwd $lcwdn",
                &info,
                "/tmp/work/"
            )
            .0,
            "antek@example.org sftp antek /tmp/work/ work"
        );
        assert_eq!(
            format("xfer$location> ", &info, "/").0,
            "xfer[sftp:/home/antek/]> "
        );
    }

    #[test]
    fn disconnected_roots_and_literal_dollars() {
        let mut info = SessionInfo::default();
        assert_eq!(format("xfer$location> ", &info, "/").0, "xfer> ");
        assert_eq!(format("$user$host$cwd$cwdn$proto", &info, "/").0, "");
        info.cwd = Some("/".into());
        assert_eq!(format("$cwdn $lcwdn", &info, "/").0, "/ /");
        assert_eq!(
            format("$$cwd $unknown ${unknown} $ ${", &info, "/").0,
            "$cwd $unknown ${unknown} $ ${"
        );
        assert_eq!(format("", &info, "/").0, "");
    }

    #[test]
    fn values_are_not_expanded_and_controls_are_escaped() {
        let info = SessionInfo {
            user: "$host\n\x1b".into(),
            ..Default::default()
        };
        assert_eq!(format("$user\r", &info, "/").0, "$host\\n\\u{1b}\\r");
    }
    #[test]
    fn spans_mark_only_expanded_paths_including_escaped_controls() {
        let info = SessionInfo {
            protocol: Some("sftp".into()),
            cwd: Some("/very/long/\nname/".into()),
            ..Default::default()
        };
        let (text, ranges) = format("$user [$cwd] fixed $location end $lcwd", &info, "/tmp/界");
        let parts = ranges
            .iter()
            .map(|&(a, b)| &text[a as usize..b as usize])
            .collect::<Vec<_>>();
        assert_eq!(
            parts,
            vec!["/very/long/\\nname/", "/very/long/\\nname/", "/tmp/界"]
        );
    }
}
