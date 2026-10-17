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

pub(crate) fn format(template: &str, info: &SessionInfo, local: &str) -> String {
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
            output.push_str(value);
        } else {
            output.push('$');
            output.push_str(&rest[..consumed]);
        }
        rest = &rest[consumed..];
    }
    output.push_str(rest);
    // Neither endpoint data nor configuration may inject terminal controls.
    output
        .chars()
        .flat_map(|ch| {
            if ch.is_control() {
                ch.escape_default().collect::<Vec<_>>()
            } else {
                vec![ch]
            }
        })
        .collect()
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
            format("xfer[$cwd]> ", &info, "/tmp/work"),
            "xfer[sftp:/home/antek/]> "
        );
        assert_eq!(
            format(
                "${user}@$host $proto $cwdn $lcwd $lcwdn",
                &info,
                "/tmp/work/"
            ),
            "antek@example.org sftp antek /tmp/work/ work"
        );
        assert_eq!(
            format("xfer$location> ", &info, "/"),
            "xfer[sftp:/home/antek/]> "
        );
    }

    #[test]
    fn disconnected_roots_and_literal_dollars() {
        let mut info = SessionInfo::default();
        assert_eq!(format("xfer$location> ", &info, "/"), "xfer> ");
        assert_eq!(format("$user$host$cwd$cwdn$proto", &info, "/"), "");
        info.cwd = Some("/".into());
        assert_eq!(format("$cwdn $lcwdn", &info, "/"), "/ /");
        assert_eq!(
            format("$$cwd $unknown ${unknown} $ ${", &info, "/"),
            "$cwd $unknown ${unknown} $ ${"
        );
        assert_eq!(format("", &info, "/"), "");
    }

    #[test]
    fn values_are_not_expanded_and_controls_are_escaped() {
        let info = SessionInfo {
            user: "$host\n\x1b".into(),
            ..Default::default()
        };
        assert_eq!(format("$user\r", &info, "/"), "$host\\n\\u{1b}\\r");
    }
}
