//! Real HTTP exchanges against an isolated, in-memory WebDAV fixture. No external
//! services or optional system packages are required.
use base64::Engine;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server,
};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};
use xfer::{
    backend::{Backend, ConnectOptions, Connection},
    core::{JobId, JobState, Settings},
    credentials::Password,
    daemon::Daemon,
    error::NonRetryableError,
    listing::{Content, ListingSink},
    transfer::DownloadSink,
    webdav::Webdav,
};
const SECRET: &str = "webdav-fixture-password";
const DATE: &str = "Wed, 01 Jan 2025 00:00:00 GMT";
#[derive(Clone)]
struct Node {
    directory: bool,
    data: Vec<u8>,
    revision: u64,
}
#[derive(Clone, Copy, PartialEq, Default)]
enum Mode {
    #[default]
    Normal,
    IgnoreRange,
    BadRange,
    ChangedEtag,
    Compressed,
    PartialOnce,
    SlowOnce,
    RedirectForeign,
    RedirectSlash,
    MissingValidator,
    LastModifiedOnly,
    UnquotedTags,
    Chunked,
    ChangedAfterGet,
    MoveConflict,
    MoveMultiStatus,
    SlowMove,
    MoveRedirect,
    MoveSourceChanged,
    MoveDestinationChanged,
    IgnoreDavIf,
}
#[derive(Clone)]
struct Call {
    method: String,
    path: String,
    range: Option<String>,
    conditional: Option<String>,
    exclusive: bool,
    depth: Option<String>,
    destination: Option<String>,
    overwrite: Option<String>,
    tagged_if: Option<String>,
}
struct State {
    nodes: BTreeMap<String, Node>,
    calls: Vec<Call>,
    auth: bool,
    mode: Mode,
    xml: Option<String>,
}
struct Fixture {
    url: String,
    state: Arc<Mutex<State>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
fn xml_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
fn href(path: &str) -> String {
    let mut url = url::Url::parse("http://localhost/").unwrap();
    {
        let mut segments = url.path_segments_mut().unwrap();
        segments.clear();
        for part in path.trim_start_matches('/').split('/') {
            segments.push(part);
        }
    }
    url.path().into()
}
fn resource(path: &str, node: &Node, validator: bool) -> String {
    format!("<x:response><x:href>{}</x:href><x:propstat><x:prop><x:getcontentlength>not-a-number</x:getcontentlength></x:prop><x:status>HTTP/1.1 404 Not Found</x:status></x:propstat><x:propstat><x:prop><x:resourcetype>{}</x:resourcetype><x:getcontentlength>{}</x:getcontentlength>{}</x:prop><x:status>HTTP/1.1 200 OK</x:status></x:propstat></x:response>", xml_escape(&href(path)), if node.directory { "<x:collection/>" } else { "" }, node.data.len(), if validator { format!("<x:getlastmodified>{DATE}</x:getlastmodified><x:getetag>&quot;v{}&quot;</x:getetag>", node.revision) } else { String::new() })
}
fn response(code: u16, body: impl Into<Body>) -> Response<Body> {
    Response::builder().status(code).body(body.into()).unwrap()
}
async fn handle(
    request: Request<Body>,
    state: Arc<Mutex<State>>,
) -> Result<Response<Body>, Infallible> {
    let method = request.method().as_str().to_owned();
    let path = percent_encoding::percent_decode_str(request.uri().path())
        .decode_utf8()
        .unwrap()
        .into_owned();
    let header = |name: &str| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    let call = Call {
        method: method.clone(),
        path: path.clone(),
        range: header("range"),
        conditional: header("if-match").or_else(|| header("if-unmodified-since")),
        exclusive: header("if-none-match").as_deref() == Some("*"),
        depth: header("depth"),
        destination: header("destination"),
        overwrite: header("overwrite"),
        tagged_if: header("if"),
    };
    let auth = header("authorization");
    let (mode, xml) = {
        let mut state = state.lock().unwrap();
        state.calls.push(call.clone());
        if state.auth
            && auth
                != Some(format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(format!("alice:{SECRET}"))
                ))
        {
            return Ok(response(401, format!("do not print {SECRET}")));
        }
        (state.mode, state.xml.clone())
    };
    if mode == Mode::RedirectForeign {
        return Ok(Response::builder()
            .status(302)
            .header("location", "http://user:remote-secret@127.0.0.1:1/stolen/")
            .body(Body::empty())
            .unwrap());
    }
    let lookup = {
        let state = state.lock().unwrap();
        if state.nodes.contains_key(&path) {
            path.clone()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        }
    };
    if mode == Mode::RedirectSlash && path != lookup {
        return Ok(Response::builder()
            .status(301)
            .header("location", href(&lookup))
            .body(Body::empty())
            .unwrap());
    }
    match method.as_str() {
        "PROPFIND" => {
            let state = state.lock().unwrap();
            let Some(node) = state.nodes.get(&lookup) else {
                return Ok(response(404, "missing"));
            };
            if call.depth.as_deref() == Some("1") {
                if let Some(xml) = xml {
                    return Ok(response(207, xml));
                }
            }
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"utf-8\"?><x:multistatus xmlns:x=\"DAV:\">{}",
                resource(&lookup, node, mode != Mode::MissingValidator)
            );
            if node.directory && call.depth.as_deref() == Some("1") {
                for (name, child) in &state.nodes {
                    if let Some(rest) = name.strip_prefix(&lookup) {
                        if !rest.is_empty() && !rest.trim_end_matches('/').contains('/') {
                            xml.push_str(&resource(name, child, mode != Mode::MissingValidator));
                        }
                    }
                }
            }
            xml.push_str("</x:multistatus>");
            if mode == Mode::LastModifiedOnly {
                xml = xml.replace("<x:getetag>&quot;v1&quot;</x:getetag>", "");
            }
            if mode == Mode::UnquotedTags {
                xml = xml.replace("&quot;", "");
            }
            Ok(response(207, xml))
        }
        "GET" => {
            let mut state = state.lock().unwrap();
            let Some(node) = state.nodes.get(&lookup).cloned() else {
                return Ok(response(404, "missing"));
            };
            if node.directory {
                return Ok(response(405, "collection"));
            }
            if call.conditional.as_deref().is_some_and(|condition| {
                condition != format!("\"v{}\"", node.revision) && condition != DATE
            }) {
                return Ok(response(412, "changed"));
            }
            let offset = call
                .range
                .as_deref()
                .and_then(|value| value.strip_prefix("bytes="))
                .and_then(|value| value.strip_suffix('-'))
                .map(|value| value.parse::<usize>().unwrap())
                .unwrap_or(0);
            let offset = if mode == Mode::IgnoreRange { 0 } else { offset };
            let mut builder = Response::builder().status(if offset > 0 { 206 } else { 200 });
            if mode != Mode::Chunked {
                builder = builder.header("content-length", node.data.len() - offset);
            }
            if mode != Mode::MissingValidator {
                builder = builder
                    .header(
                        "etag",
                        format!(
                            "\"v{}\"",
                            node.revision + u64::from(mode == Mode::ChangedEtag)
                        ),
                    )
                    .header("last-modified", DATE);
            }
            if mode == Mode::LastModifiedOnly {
                builder.headers_mut().unwrap().remove("etag");
            }
            if mode == Mode::Compressed {
                builder = builder.header("content-encoding", "gzip");
            }
            if offset > 0 {
                builder = builder.header(
                    "content-range",
                    format!(
                        "bytes {}-{}/{}",
                        offset + usize::from(mode == Mode::BadRange),
                        node.data.len() - 1,
                        node.data.len()
                    ),
                );
            }
            let body = if matches!(mode, Mode::PartialOnce | Mode::SlowOnce) {
                state.mode = Mode::Normal;
                let first = node.data[offset..offset + 8192].to_vec();
                let stream = futures_util::stream::once(async { Ok::<_, std::io::Error>(first) })
                    .chain(futures_util::stream::once(async move {
                        if mode == Mode::SlowOnce {
                            tokio::time::sleep(Duration::from_secs(600)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Err(std::io::Error::other("fixture disconnect"))
                    }));
                Body::wrap_stream(stream)
            } else if mode == Mode::Chunked {
                Body::wrap_stream(futures_util::stream::iter(
                    node.data[offset..]
                        .chunks(7000)
                        .map(|bytes| Ok::<_, std::io::Error>(bytes.to_vec()))
                        .collect::<Vec<_>>(),
                ))
            } else {
                Body::from(node.data[offset..].to_vec())
            };
            if mode == Mode::ChangedAfterGet {
                state.nodes.get_mut(&lookup).unwrap().revision += 1;
            }
            Ok(builder.body(body).unwrap())
        }
        "PUT" => {
            let data = hyper::body::to_bytes(request.into_body())
                .await
                .unwrap()
                .to_vec();
            let mut state = state.lock().unwrap();
            if call.exclusive && state.nodes.contains_key(&path) {
                return Ok(response(412, "exists"));
            }
            let parent = format!("{}/", path.rsplit_once('/').unwrap().0);
            if !state.nodes.get(&parent).is_some_and(|node| node.directory) {
                return Ok(response(409, "missing parent"));
            }
            let revision = state
                .nodes
                .get(&path)
                .map(|node| node.revision + 1)
                .unwrap_or(1);
            state.nodes.insert(
                path,
                Node {
                    directory: false,
                    data,
                    revision,
                },
            );
            Ok(response(201, Body::empty()))
        }
        "HEAD" => {
            let state = state.lock().unwrap();
            let Some(node) = state.nodes.get(&lookup) else {
                return Ok(response(404, "missing"));
            };
            if mode != Mode::IgnoreDavIf
                && call.tagged_if.as_ref().is_some_and(|tagged| {
                    tagged.ends_with(&format!("(Not [\"v{}\"])", node.revision))
                })
            {
                return Ok(response(412, "condition is false"));
            }
            Ok(Response::builder()
                .status(200)
                .header("content-length", node.data.len())
                .body(Body::empty())
                .unwrap())
        }
        "MOVE" => {
            if mode == Mode::MoveRedirect {
                return Ok(Response::builder()
                    .status(307)
                    .header("location", href("/dav/hello.bin/"))
                    .body(Body::empty())
                    .unwrap());
            }
            if mode == Mode::SlowMove {
                tokio::time::sleep(Duration::from_secs(600)).await;
            }
            if mode == Mode::MoveMultiStatus {
                return Ok(response(207, "<multistatus/>"));
            }
            let Some(destination) = call
                .destination
                .as_deref()
                .and_then(|value| url::Url::parse(value).ok())
            else {
                return Ok(response(400, "missing destination"));
            };
            let destination = percent_encoding::percent_decode_str(destination.path())
                .decode_utf8()
                .unwrap()
                .into_owned();
            let mut state = state.lock().unwrap();
            if mode == Mode::MoveSourceChanged {
                state.nodes.get_mut(&lookup).unwrap().revision += 1;
            }
            if mode == Mode::MoveDestinationChanged {
                let node = state.nodes.get_mut(&destination).unwrap();
                node.revision += 1;
                node.directory = true;
            }
            let Some(source) = state.nodes.get(&lookup).cloned() else {
                return Ok(response(404, "missing source"));
            };
            if source.directory {
                return Ok(response(409, "collection"));
            }
            if mode == Mode::MoveConflict {
                return Ok(response(412, "changed"));
            }
            if call
                .conditional
                .as_deref()
                .is_some_and(|tag| tag != format!("\"v{}\"", source.revision) && tag != DATE)
            {
                return Ok(response(412, "source changed"));
            }
            let exists = state.nodes.contains_key(&destination);
            if exists && call.overwrite.as_deref() != Some("T") {
                return Ok(response(412, "exists"));
            }
            if let Some(tagged) = &call.tagged_if {
                if !state.nodes.get(&destination).is_some_and(|node| {
                    tagged.contains(&format!(
                        "<{}> ([\"v{}\"])",
                        call.destination.as_deref().unwrap(),
                        node.revision
                    ))
                }) {
                    return Ok(response(412, "destination changed"));
                }
                if !tagged.ends_with(&format!("([\"v{}\"])", source.revision)) {
                    return Ok(response(412, "source changed"));
                }
            }
            let parent = format!("{}/", destination.rsplit_once('/').unwrap().0);
            if !state.nodes.get(&parent).is_some_and(|node| node.directory) {
                return Ok(response(409, "missing parent"));
            }
            if state
                .nodes
                .get(&destination)
                .is_some_and(|node| node.directory)
            {
                return Ok(response(409, "directory target"));
            }
            state.nodes.remove(&lookup);
            state.nodes.insert(destination, source);
            Ok(response(if exists { 204 } else { 201 }, Body::empty()))
        }
        "MKCOL" => {
            let mut state = state.lock().unwrap();
            if state.nodes.contains_key(&lookup) {
                return Ok(response(405, "exists"));
            }
            let parent = format!(
                "{}/",
                path.trim_end_matches('/').rsplit_once('/').unwrap().0
            );
            if !state.nodes.get(&parent).is_some_and(|node| node.directory) {
                return Ok(response(409, "missing parent"));
            }
            state.nodes.insert(
                lookup,
                Node {
                    directory: true,
                    data: Vec::new(),
                    revision: 1,
                },
            );
            Ok(response(201, Body::empty()))
        }
        _ => Ok(response(405, "unsupported")),
    }
}
use futures_util::StreamExt;
impl Fixture {
    fn start(auth: bool) -> Self {
        let mut nodes = BTreeMap::new();
        for path in ["/", "/dav/", "/dav/dir/"] {
            nodes.insert(
                path.into(),
                Node {
                    directory: true,
                    data: Vec::new(),
                    revision: 1,
                },
            );
        }
        for (path, data) in [
            (
                "/dav/hello.bin",
                (0..300_123).map(|i| (i % 251) as u8).collect(),
            ),
            ("/dav/雪 %?#.txt", b"unicode file".to_vec()),
            ("/dav/dir/nested.txt", b"nested".to_vec()),
        ] {
            nodes.insert(
                path.into(),
                Node {
                    directory: false,
                    data,
                    revision: 1,
                },
            );
        }
        let state = Arc::new(Mutex::new(State {
            nodes,
            calls: Vec::new(),
            auth,
            mode: Mode::Normal,
            xml: None,
        }));
        let copy = state.clone();
        let server = Server::bind(&"127.0.0.1:0".parse().unwrap());
        let address = server.local_addr();
        let server =
            server.serve(make_service_fn(move |_| {
                let state = copy.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |request| handle(request, state.clone())))
                }
            }));
        let task = tokio::spawn(async move {
            let _ = server.await;
        });
        Self {
            url: format!("http://{address}/dav/"),
            state,
            task,
        }
    }
    fn settings(&self) -> Settings {
        let mut settings = Settings::new();
        Webdav.register_settings(&mut settings);
        settings.set("webdav:allow-insecure-auth", "true").unwrap();
        settings
    }
    async fn connect(&self) -> Box<dyn Connection> {
        Webdav
            .connect(ConnectOptions {
                target: self.url.clone(),
                bookmark: None,
                settings: self.settings(),
                password: None,
            })
            .await
            .unwrap()
    }
    fn mode(&self, mode: Mode) {
        self.state.lock().unwrap().mode = mode;
    }
}
#[derive(Default)]
struct Records(Vec<Content>);
impl ListingSink for Records {
    fn emit(&mut self, entry: Content) -> anyhow::Result<()> {
        self.0.push(entry);
        Ok(())
    }
}
#[derive(Default)]
struct BytesSink(Vec<u8>);
impl DownloadSink for BytesSink {
    fn write(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        assert!(bytes.len() <= 65536);
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}

#[tokio::test]
async fn navigation_listing_metadata_unicode_and_home_retention() {
    let server = Fixture::start(false);
    let mut connection = server.connect().await;
    assert_eq!(connection.pwd().unwrap(), "/dav/");
    let mut records = Records::default();
    assert_eq!(connection.list("", &mut records).await.unwrap(), 3);
    assert!(records.0.iter().any(|entry| matches!(entry, Content::Directory(info) if info.name == "dir" && info.unix.is_none())));
    assert!(records.0.iter().any(|entry| matches!(entry, Content::File(info) if info.name == "hello.bin" && info.size == Some(300123) && info.modified.is_some())));
    let mut records = Records::default();
    connection.list("雪 %?#.txt", &mut records).await.unwrap();
    assert_eq!(records.0.len(), 1);
    let expected = connection.stat_file("雪 %?#.txt").await.unwrap();
    let mut sink = BytesSink::default();
    connection
        .download("雪 %?#.txt", 0, &expected, &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.0, b"unicode file");
    connection.change_directory("dir").await.unwrap();
    assert_eq!(connection.pwd().unwrap(), "/dav/dir/");
    assert!(connection
        .change_directory("missing")
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert_eq!(connection.pwd().unwrap(), "/dav/dir/");
    connection
        .change_directory(&connection.resolve_directory(None).unwrap())
        .await
        .unwrap();
    assert_eq!(connection.pwd().unwrap(), "/dav/");
    server.mode(Mode::RedirectSlash);
    connection
        .list("dir", &mut Records::default())
        .await
        .unwrap();
    assert!(connection
        .stat_file("dir/")
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
}
#[tokio::test]
async fn transfers_resume_after_disconnect_or_cancellation_and_reject_changed_files() {
    let server = Fixture::start(false);
    let connection = server.connect().await;
    let expected = connection.stat_file("hello.bin").await.unwrap();
    server.mode(Mode::PartialOnce);
    let mut sink = BytesSink::default();
    let error = connection
        .download("hello.bin", 0, &expected, &mut sink)
        .await
        .unwrap_err();
    assert!(!error.is::<NonRetryableError>());
    assert!(!sink.0.is_empty() && sink.0.len() < expected.size as usize);
    let offset = sink.0.len() as u64;
    connection
        .download("hello.bin", offset, &expected, &mut sink)
        .await
        .unwrap();
    assert_eq!(
        sink.0,
        server.state.lock().unwrap().nodes["/dav/hello.bin"].data
    );
    assert!(server
        .state
        .lock()
        .unwrap()
        .calls
        .iter()
        .any(|call| call.range == Some(format!("bytes={offset}-"))
            && call.conditional.as_deref() == Some("\"v1\"")));
    server.mode(Mode::SlowOnce);
    let mut sink = BytesSink::default();
    assert!(tokio::time::timeout(
        Duration::from_millis(80),
        connection.download("hello.bin", 0, &expected, &mut sink)
    )
    .await
    .is_err());
    assert!(!sink.0.is_empty());
    connection
        .download("hello.bin", sink.0.len() as u64, &expected, &mut sink)
        .await
        .unwrap();
    assert_eq!(sink.0.len() as u64, expected.size);
    server
        .state
        .lock()
        .unwrap()
        .nodes
        .get_mut("/dav/hello.bin")
        .unwrap()
        .revision += 1;
    let mut sink = BytesSink::default();
    assert!(connection
        .download("hello.bin", offset, &expected, &mut sink)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert!(sink.0.is_empty());
}
#[tokio::test]
async fn unsafe_ranges_encoding_validators_redirects_and_href_scopes_fail_before_writing() {
    let server = Fixture::start(false);
    let connection = server.connect().await;
    let expected = connection.stat_file("hello.bin").await.unwrap();
    for mode in [
        Mode::IgnoreRange,
        Mode::BadRange,
        Mode::ChangedEtag,
        Mode::Compressed,
        Mode::RedirectForeign,
    ] {
        server.mode(mode);
        let mut sink = BytesSink::default();
        let error = connection
            .download("hello.bin", 10, &expected, &mut sink)
            .await
            .unwrap_err();
        assert!(error.is::<NonRetryableError>());
        assert!(sink.0.is_empty());
        assert!(!format!("{error:#}").contains("remote-secret"));
    }
    server.mode(Mode::MissingValidator);
    let expected = connection.stat_file("hello.bin").await.unwrap();
    let mut sink = BytesSink::default();
    assert!(connection
        .download("hello.bin", 1, &expected, &mut sink)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert!(sink.0.is_empty());
    connection
        .download("hello.bin", 0, &expected, &mut sink)
        .await
        .unwrap();
    server.mode(Mode::Normal);
    for href in [
        "http://127.0.0.1:1/stolen",
        "/outside/file",
        "/dav/dir/nested.txt",
        "/dav/bad%2Fname",
        "/dav/bad%00name",
    ] {
        server.state.lock().unwrap().xml = Some(format!("<multistatus xmlns='DAV:'><response><href>{href}</href><propstat><prop><resourcetype/></prop><status>HTTP/1.1 200 OK</status></propstat></response></multistatus>"));
        assert!(connection
            .list("", &mut Records::default())
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
    }
}
#[tokio::test]
async fn uploads_are_exclusive_unless_clobber_and_mkdir_checks_existing_types() {
    let server = Fixture::start(false);
    let connection = server.connect().await;
    let local = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(local.path(), b"upload \x00\xff content").unwrap();
    connection
        .upload(local.path(), "new.bin", false)
        .await
        .unwrap();
    assert!(connection
        .upload(local.path(), "new.bin", false)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    std::fs::write(local.path(), b"replacement").unwrap();
    connection
        .upload(local.path(), "new.bin", true)
        .await
        .unwrap();
    assert_eq!(
        server.state.lock().unwrap().nodes["/dav/new.bin"].data,
        b"replacement"
    );
    connection.create_directory("one", false).await.unwrap();
    assert!(connection
        .create_directory("one", false)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    connection
        .create_directory("one/two/three", true)
        .await
        .unwrap();
    connection
        .create_directory("one/two/three", true)
        .await
        .unwrap();
    assert!(connection
        .create_directory("new.bin/child", true)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert!(connection
        .create_directory("absent/child", false)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert_eq!(connection.pwd().unwrap(), "/dav/");
    let calls = &server.state.lock().unwrap().calls;
    assert!(calls
        .iter()
        .any(|call| call.method == "PUT" && call.exclusive));
    assert!(calls
        .iter()
        .any(|call| call.method == "PUT" && !call.exclusive));
    assert!(calls
        .iter()
        .any(|call| call.method == "MKCOL" && call.path.ends_with("three/")));
}
async fn finish(app: &mut Daemon, lines: Vec<String>, success: bool) -> JobId {
    let id: JobId = lines[0]
        .strip_prefix("JOB ")
        .unwrap_or_else(|| panic!("unexpected result: {lines:?}"))
        .parse()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.has_running_jobs() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        app.jobs.jobs[&id].state == JobState::Completed,
        success,
        "{}",
        app.jobs.jobs[&id].output.read_all().join("\n")
    );
    assert!(!app.jobs.jobs[&id].title.contains(SECRET));
    assert!(!app.jobs.jobs[&id]
        .output
        .read_all()
        .join("\n")
        .contains(SECRET));
    id
}
#[tokio::test]
async fn credentials_policy_stored_password_prompt_override_and_daemon_commands() {
    let server = Fixture::start(true);
    let root = tempfile::tempdir().unwrap();
    let book = root.path().join("bookmarks.toml");
    std::fs::write(&book, format!("[dav]\ntype='webdav'\nurl='{}'\nuser='alice'\npassword='{SECRET}'\n[ask]\ntype='webdav'\nurl='{}'\nuser='alice'\npassword='wrong-password'\npassword_interactive=true\n", server.url, server.url)).unwrap();
    let mut app = Daemon::bootstrap().unwrap();
    app.command(&format!("set bookmarks:file {}", book.display()));
    let error = app.command("open dav").join("\n");
    assert!(error.contains("require HTTPS"));
    assert!(!error.contains(SECRET));
    app.command("set webdav:allow-insecure-auth true");
    let lines = app.command("open dav");
    finish(&mut app, lines, true).await;
    let lines = app.command("cat '雪 %?#.txt'");
    let id = finish(&mut app, lines, true).await;
    assert_eq!(
        app.jobs.jobs[&id].output.read_bytes(0).unwrap(),
        b"unicode file"
    );
    for command in ["ls", "du", "mkdir -p created/child"] {
        let lines = app.command(command);
        finish(&mut app, lines, true).await;
    }
    let completion = app.path_completion("created/".into(), false).await.unwrap();
    assert!(completion
        .candidates
        .iter()
        .any(|path| path == "created/child/"));
    app.set_local_directory(root.path().to_path_buf()).unwrap();
    let lines = app.command("get '雪 %?#.txt'");
    finish(&mut app, lines, true).await;
    assert_eq!(
        std::fs::read(root.path().join("雪 %?#.txt")).unwrap(),
        b"unicode file"
    );
    std::fs::write(root.path().join("local.txt"), b"local bytes").unwrap();
    let lines = app.command("put local.txt");
    finish(&mut app, lines, true).await;
    assert!(app.command("open ask")[0].starts_with("PASSWORD_REQUIRED "));
    let lines = app.connect_command("ask", Some(Password::new(SECRET.into())));
    finish(&mut app, lines, true).await;
    let url = server
        .url
        .replacen("http://", &format!("http://alice:{SECRET}@"), 1);
    let lines = app.command(&format!("open {url}"));
    finish(&mut app, lines, true).await;
    let wrong = server
        .url
        .replacen("http://", "http://alice:wrong-password@", 1);
    let lines = app.command(&format!("open {wrong}"));
    finish(&mut app, lines, false).await;
    for command in ["bookmark", "jobs", "status", "show"] {
        assert!(!app.command(command).join("\n").contains(SECRET));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_verification_is_default_and_explicit_skip_is_connection_local() {
    use std::io::{BufRead, Write};
    let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let tls = Arc::new(
        rustls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(
                vec![rustls::Certificate(cert.cert.der().to_vec())],
                rustls::PrivateKey(cert.key_pair.serialize_der()),
            )
            .unwrap(),
    );
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let thread = std::thread::spawn(move || {
        for _ in 0..2 {
            let (socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let session = rustls::ServerConnection::new(tls.clone()).unwrap();
            let mut reader = std::io::BufReader::new(rustls::StreamOwned::new(session, socket));
            let mut request = String::new();
            if reader.read_line(&mut request).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut line = String::new();
                if reader.read_line(&mut line).unwrap() == 0 {
                    break;
                }
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = value.trim().parse::<usize>().unwrap();
                }
            }
            let mut body = vec![0; length];
            std::io::Read::read_exact(&mut reader, &mut body).unwrap();
            let xml = "<multistatus xmlns='DAV:'><response><href>/</href><propstat><prop><resourcetype><collection/></resourcetype></prop><status>HTTP/1.1 200 OK</status></propstat></response></multistatus>";
            write!(
                reader.get_mut(),
                "HTTP/1.1 207 Multi-Status\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{xml}",
                xml.len()
            )
            .unwrap();
            reader.get_mut().flush().unwrap();
        }
    });
    let mut settings = Settings::new();
    Webdav.register_settings(&mut settings);
    settings.register(
        "ssl:skip-validation",
        xfer::core::SettingValue::Bool(false),
        "test",
    );
    let options = ConnectOptions {
        target: format!("https://{address}/"),
        bookmark: None,
        settings: settings.clone(),
        password: None,
    };
    let error = match Webdav.connect(options.clone()).await {
        Ok(_) => panic!("self-signed certificate accepted"),
        Err(error) => error,
    };
    assert!(error.is::<NonRetryableError>(), "{error:#}");
    settings.set("ssl:skip-validation", "true").unwrap();
    Webdav
        .connect(ConnectOptions {
            settings,
            ..options
        })
        .await
        .unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn response_header_timeout_is_retryable_and_bounded() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
    });
    let mut settings = Settings::new();
    Webdav.register_settings(&mut settings);
    settings.set("webdav:operation-timeout", "1").unwrap();
    let start = std::time::Instant::now();
    let error = match Webdav
        .connect(ConnectOptions {
            target: format!("http://{address}/"),
            bookmark: None,
            settings,
            password: None,
        })
        .await
    {
        Ok(_) => panic!("silent server accepted"),
        Err(error) => error,
    };
    assert!(!error.is::<NonRetryableError>());
    assert!(start.elapsed() < Duration::from_secs(3));
    task.abort();
}

#[tokio::test]
async fn chunked_downloads_mtime_fallback_and_post_read_changes_are_checked() {
    let server = Fixture::start(false);
    let connection = server.connect().await;
    for mode in [Mode::Chunked, Mode::LastModifiedOnly, Mode::UnquotedTags] {
        server.mode(mode);
        let expected = connection.stat_file("hello.bin").await.unwrap();
        let mut sink = BytesSink::default();
        connection
            .download("hello.bin", 17, &expected, &mut sink)
            .await
            .unwrap();
        assert_eq!(
            sink.0,
            server.state.lock().unwrap().nodes["/dav/hello.bin"].data[17..]
        );
    }
    assert!(server
        .state
        .lock()
        .unwrap()
        .calls
        .iter()
        .any(|call| call.conditional.as_deref() == Some(DATE)));
    server.mode(Mode::ChangedAfterGet);
    let expected = connection.stat_file("hello.bin").await.unwrap();
    let mut sink = BytesSink::default();
    assert!(connection
        .download("hello.bin", 0, &expected, &mut sink)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert_eq!(sink.0.len() as u64, expected.size);
}

#[tokio::test]
async fn moves_use_same_origin_encoded_destinations_and_explicit_overwrite_conditions() {
    let fixture = Fixture::start(false);
    let connection = fixture.connect().await;
    connection
        .rename_file("hello.bin", "./hello.bin", false)
        .await
        .unwrap();
    let expected = fixture.state.lock().unwrap().nodes["/dav/hello.bin"]
        .data
        .clone();
    assert!(connection
        .rename_file("hello.bin", "雪 %?#.txt", false)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert_eq!(
        fixture.state.lock().unwrap().nodes["/dav/雪 %?#.txt"].data,
        b"unicode file"
    );
    connection
        .rename_file("hello.bin", "dir/new 雪 %?#.bin", false)
        .await
        .unwrap();
    assert!(!fixture
        .state
        .lock()
        .unwrap()
        .nodes
        .contains_key("/dav/hello.bin"));
    assert_eq!(
        fixture.state.lock().unwrap().nodes["/dav/dir/new 雪 %?#.bin"].data,
        expected
    );
    connection
        .rename_file("dir/new 雪 %?#.bin", "雪 %?#.txt", true)
        .await
        .unwrap();
    assert_eq!(
        fixture.state.lock().unwrap().nodes["/dav/雪 %?#.txt"].data,
        expected
    );
    assert_eq!(connection.pwd().unwrap(), "/dav/");
    for (a, b, overwrite) in [
        ("dir", "new", false),
        ("雪 %?#.txt", "dir", true),
        ("missing", "new", false),
        ("雪 %?#.txt", "missing/new", false),
    ] {
        assert!(connection
            .rename_file(a, b, overwrite)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
    }
    let state = fixture.state.lock().unwrap();
    let moves = state
        .calls
        .iter()
        .filter(|call| call.method == "MOVE")
        .collect::<Vec<_>>();
    assert_eq!(moves.len(), 4);
    assert_eq!(moves[1].overwrite.as_deref(), Some("F"));
    let destination = moves[1].destination.as_deref().unwrap();
    assert!(
        destination.contains("/dav/dir/new%20%E9%9B%AA%20%25%3F%23.bin"),
        "{destination}"
    );
    let target = url::Url::parse(destination).unwrap();
    let base = url::Url::parse(&fixture.url).unwrap();
    assert_eq!(target.origin(), base.origin());
    assert!(target.username().is_empty());
    assert!(target.password().is_none());
    assert_eq!(moves[2].overwrite.as_deref(), Some("T"));
    assert!(moves[2]
        .tagged_if
        .as_deref()
        .unwrap()
        .ends_with("([\"v1\"])"));
    assert!(moves[2].conditional.is_none());
    assert!(moves[2]
        .tagged_if
        .as_deref()
        .unwrap()
        .contains("/dav/dir/new%20%E9%9B%AA%20%25%3F%23.bin> ([\"v1\"])"));
}

#[tokio::test]
async fn moves_fail_closed_on_conflicts_multistatus_missing_replacement_validators_and_redirects() {
    for mode in [
        Mode::MoveConflict,
        Mode::MoveMultiStatus,
        Mode::RedirectForeign,
        Mode::MoveRedirect,
        Mode::MoveSourceChanged,
    ] {
        let fixture = Fixture::start(false);
        let connection = fixture.connect().await;
        fixture.mode(mode);
        assert!(connection
            .rename_file("hello.bin", "new", false)
            .await
            .unwrap_err()
            .is::<NonRetryableError>());
        let state = fixture.state.lock().unwrap();
        assert!(state.nodes.contains_key("/dav/hello.bin"));
        assert!(!state.nodes.contains_key("/dav/new"));
        assert!(
            state
                .calls
                .iter()
                .filter(|call| call.method == "MOVE")
                .count()
                <= 1
        );
        assert!(state
            .calls
            .iter()
            .all(|call| !matches!(call.method.as_str(), "PUT" | "DELETE")));
    }
    let fixture = Fixture::start(false);
    let connection = fixture.connect().await;
    fixture.mode(Mode::MissingValidator);
    assert!(connection
        .rename_file("hello.bin", "雪 %?#.txt", true)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    assert!(!fixture
        .state
        .lock()
        .unwrap()
        .calls
        .iter()
        .any(|call| call.method == "MOVE"));
    connection
        .rename_file("hello.bin", "new", false)
        .await
        .unwrap();
    assert!(fixture.state.lock().unwrap().nodes.contains_key("/dav/new"));
}

#[tokio::test]
async fn cancelled_moves_are_not_replayed_and_cache_refreshes_after_moves() {
    let fixture = Fixture::start(false);
    let connection = fixture.connect().await;
    fixture.mode(Mode::SlowMove);
    assert!(tokio::time::timeout(
        Duration::from_millis(60),
        connection.rename_file("hello.bin", "new", false)
    )
    .await
    .is_err());
    assert_eq!(
        fixture
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .filter(|call| call.method == "MOVE")
            .count(),
        1
    );
    assert!(fixture
        .state
        .lock()
        .unwrap()
        .nodes
        .contains_key("/dav/hello.bin"));
    // A fresh connection can operate normally after a cancelled request; no
    // deferred command-level retry is scheduled by the move implementation.
    fixture.mode(Mode::Normal);
    let app = Daemon::bootstrap().unwrap();
    let connection = xfer::cache::CachedConnection::new(
        fixture.connect().await,
        xfer::cache::CacheOptions::from_settings(&app.settings).unwrap(),
    );
    assert_eq!(
        connection.complete_paths("hello").await.unwrap().candidates,
        vec!["hello.bin"]
    );
    connection
        .rename_file("hello.bin", "dir/renamed file", false)
        .await
        .unwrap();
    assert!(connection
        .complete_paths("hello")
        .await
        .unwrap()
        .candidates
        .is_empty());
    assert_eq!(
        connection
            .complete_paths("dir/renamed")
            .await
            .unwrap()
            .candidates,
        vec!["dir/renamed file"]
    );
}

#[tokio::test]
async fn move_replacement_does_not_delete_a_collection_substituted_after_stat() {
    let fixture = Fixture::start(false);
    let connection = fixture.connect().await;
    fixture.mode(Mode::MoveDestinationChanged);
    assert!(connection
        .rename_file("hello.bin", "雪 %?#.txt", true)
        .await
        .unwrap_err()
        .is::<NonRetryableError>());
    let state = fixture.state.lock().unwrap();
    assert!(state.nodes.contains_key("/dav/hello.bin"));
    assert!(state.nodes["/dav/雪 %?#.txt"].directory);
    assert_eq!(
        state
            .calls
            .iter()
            .filter(|call| call.method == "MOVE")
            .count(),
        1
    );
    assert!(!state.calls.iter().any(|call| call.method == "DELETE"));
}

#[tokio::test]
async fn unsafe_webdav_replacement_is_refused_when_server_ignores_tagged_conditions() {
    let fixture = Fixture::start(false);
    let connection = fixture.connect().await;
    fixture.mode(Mode::IgnoreDavIf);
    let error = connection
        .rename_file("hello.bin", "雪 %?#.txt", true)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("enforced tagged If"));
    assert!(fixture
        .state
        .lock()
        .unwrap()
        .calls
        .iter()
        .all(|call| call.method != "MOVE"));
    assert_eq!(
        fixture.state.lock().unwrap().nodes["/dav/雪 %?#.txt"].data,
        b"unicode file"
    );
    connection
        .rename_file("hello.bin", "new", true)
        .await
        .unwrap();
    assert!(fixture.state.lock().unwrap().nodes.contains_key("/dav/new"));
}
