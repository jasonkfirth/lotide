use serde_derive::Deserialize;
use std::collections::HashMap;

pub const ACTIVITY_TYPE: &str =
    "application/ld+json; profile=\"https://www.w3.org/ns/activitystreams\"";

struct TestServer {
    real_host_url: String,
    process: std::process::Child,
}

impl TestServer {
    pub fn start(idx: u16) -> Self {
        let db_url =
            std::env::var(format!("DATABASE_URL_{}", idx)).expect("Missing DATABASE_URL_#");
        let ap_host_url = format!("http://{}.lotidetests.localhost", idx);
        let port = portpicker::pick_unused_port().unwrap();
        let real_host_url = format!("http://localhost:{}", port);

        let child = std::process::Command::new(env!("CARGO_BIN_EXE_lotide"))
            .env("DATABASE_URL", db_url)
            .env("PORT", port.to_string())
            .env("HOST_URL_ACTIVITYPUB", format!("{}/apub", ap_host_url))
            .env("HOST_URL_API", format!("{}/api", ap_host_url))
            .env("DEV_MODE", "true")
            .spawn()
            .unwrap();

        let res = Self {
            real_host_url,
            process: child,
        };

        std::thread::sleep(std::time::Duration::from_secs(1));

        res
    }
}

impl std::ops::Drop for TestServer {
    fn drop(&mut self) {
        self.process.kill().unwrap();
    }
}

struct FileInfo {
    content_type: &'static str,
    content: String,
}

struct FileServer {
    url: String,
    file_map: std::sync::Arc<std::sync::RwLock<HashMap<String, FileInfo>>>,
    stop_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl FileServer {
    pub fn start() -> Self {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();

        let file_map =
            std::sync::Arc::new(std::sync::RwLock::new(HashMap::<String, FileInfo>::new()));

        let listener = std::net::TcpListener::bind(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::LOCALHOST,
            0,
        ))
        .unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();

        tokio::spawn({
            let file_map = file_map.clone();
            async move {
                let mut stop_rx = stop_rx;

                loop {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        accepted = listener.accept() => {
                            let (stream, _) = match accepted {
                                Ok(accepted) => accepted,
                                Err(err) => {
                                    eprintln!("Error occurred in test file server accept: {:?}", err);
                                    break;
                                }
                            };

                            let file_map = file_map.clone();

                            tokio::spawn(async move {
                                let io = hyper_util::rt::TokioIo::new(stream);
                                let service = hyper1::service::service_fn(
                                    move |req: hyper1::Request<hyper1::body::Incoming>| {
                                        let file_map = file_map.clone();

                                        async move {
                                            let response = {
                                                let file_map = file_map.read().unwrap();

                                                if let Some(info) = file_map.get(req.uri().path()) {
                                                    let mut res = hyper1::Response::new(
                                                        http_body_util::Full::new(
                                                            bytes::Bytes::from(info.content.clone()),
                                                        ),
                                                    );

                                                    res.headers_mut().insert(
                                                        hyper1::header::CONTENT_TYPE,
                                                        hyper1::header::HeaderValue::from_static(
                                                            info.content_type,
                                                        ),
                                                    );

                                                    res
                                                } else {
                                                    let mut res = hyper1::Response::new(
                                                        http_body_util::Full::new(
                                                            bytes::Bytes::from_static(b"not found"),
                                                        ),
                                                    );
                                                    *res.status_mut() = hyper1::StatusCode::NOT_FOUND;

                                                    res
                                                }
                                            };

                                            Result::<_, std::convert::Infallible>::Ok(response)
                                        }
                                    },
                                );

                                if let Err(err) =
                                    hyper1::server::conn::http1::Builder::new()
                                        .serve_connection(io, service)
                                        .await
                                {
                                    eprintln!("Error occurred in test file server: {:?}", err);
                                }
                            });
                        }
                    }
                }
            }
        });

        Self {
            url,
            file_map,
            stop_tx: Some(stop_tx),
        }
    }

    pub fn add_file(&self, path: String, content_type: &'static str, content: String) {
        let file_map = &mut self.file_map.write().unwrap();
        file_map.insert(
            path,
            FileInfo {
                content_type,
                content,
            },
        );
    }
}

impl std::ops::Drop for FileServer {
    fn drop(&mut self) {
        let _ = self.stop_tx.take().unwrap().send(());
    }
}

fn random_string() -> String {
    use rand::distr::{Alphanumeric, SampleString};

    Alphanumeric.sample_string(&mut rand::rng(), 16)
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct TestUserInfo {
    id: i64,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct TestCreateUserResponse {
    user: TestUserInfo,
    token: String,
}

#[allow(dead_code)]
async fn create_account(client: &reqwest::Client, server: &TestServer) -> String {
    let resp = client
        .post(format!("{}/api/unstable/users", server.real_host_url))
        .json(&serde_json::json!({
            "username": random_string(),
            "password": random_string(),
            "login": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    #[derive(Deserialize)]
    struct JustToken {
        token: String,
    }

    let resp: JustToken = resp.json().await.unwrap();

    resp.token
}

#[allow(dead_code)]
async fn create_account_with_id(client: &reqwest::Client, server: &TestServer) -> (i64, String) {
    let resp = client
        .post(format!("{}/api/unstable/users", server.real_host_url))
        .json(&serde_json::json!({
            "username": random_string(),
            "password": random_string(),
            "login": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: TestCreateUserResponse = resp.json().await.unwrap();
    (resp.user.id, resp.token)
}

#[allow(dead_code)]
struct CommunityInfo {
    id: i64,
    name: String,
}

#[allow(dead_code)]
async fn create_community(
    client: &reqwest::Client,
    server: &TestServer,
    token: &str,
) -> CommunityInfo {
    let community_name = random_string();

    let resp = client
        .post(format!("{}/api/unstable/communities", server.real_host_url))
        .bearer_auth(token)
        .json(&serde_json::json!({ "name": community_name }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: serde_json::Value = resp.json().await.unwrap();

    CommunityInfo {
        id: resp["community"]["id"].as_i64().unwrap(),
        name: community_name,
    }
}

async fn lookup_community(client: &reqwest::Client, server: &TestServer, ap_id: &str) -> i64 {
    let resp = client
        .get(format!(
            "{}/api/unstable/actors:lookup/{}",
            server.real_host_url,
            percent_encoding::utf8_percent_encode(ap_id, percent_encoding::NON_ALPHANUMERIC)
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let resp: (serde_json::Value,) = resp.json().await.unwrap();
    let (resp,) = resp;
    resp["id"].as_i64().unwrap()
}

#[tokio::test]
async fn community_fetch() {
    let server = TestServer::start(1);

    let remote_server = FileServer::start();

    let client = reqwest::Client::builder().build().unwrap();

    let path = format!("/{}", random_string());
    let ap_id = format!("{}{}", remote_server.url, path);
    let name = random_string();
    let content = serde_json::json!({
        "id": ap_id,
        "type": "Group",
        "preferredUsername": name,
        "inbox": format!("{}/inbox", ap_id),
    })
    .to_string();

    remote_server.add_file(path, ACTIVITY_TYPE, content);

    let community_remote_id = lookup_community(&client, &server, &ap_id).await;

    let resp = client
        .get(format!(
            "{}/api/unstable/communities/{}",
            server.real_host_url, community_remote_id
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let resp: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(resp["name"].as_str(), Some(name.as_ref()));
    assert_eq!(resp["local"].as_bool(), Some(false));
}

#[tokio::test]
async fn user_follow_endpoints() {
    let server = TestServer::start(1);
    let client = reqwest::Client::builder().build().unwrap();

    let (target_user, _target_token) = create_account_with_id(&client, &server).await;
    let (follower_user, follower_token) = create_account_with_id(&client, &server).await;

    let follow_response = client
        .post(format!(
            "{}/api/unstable/users/{}/follow",
            server.real_host_url, target_user
        ))
        .bearer_auth(&follower_token)
        .json(&serde_json::json!({
            "try_wait_for_accept": true
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    assert_eq!(follow_response.status(), reqwest::StatusCode::OK);

    let follow_body: serde_json::Value = follow_response.json().await.unwrap();
    assert_eq!(follow_body["accepted"].as_bool(), Some(true));

    let follow_apub = client
        .get(format!(
            "{}/apub/users/{}/followers/{}",
            server.real_host_url, target_user, follower_user
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(follow_apub["type"].as_str(), Some("Follow"));
    assert!(
        follow_apub["actor"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/apub/users/{}", follower_user))
    );

    let join_apub = client
        .get(format!(
            "{}/apub/users/{}/followers/{}/join",
            server.real_host_url, target_user, follower_user
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(join_apub["type"].as_str(), Some("Join"));

    let accept_apub = client
        .get(format!(
            "{}/apub/users/{}/followers/{}/accept",
            server.real_host_url, target_user, follower_user
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(accept_apub["type"].as_str(), Some("Accept"));
    assert_eq!(accept_apub["object"]["type"].as_str(), Some("Follow"));
    assert!(
        accept_apub["object"]["actor"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/apub/users/{}", follower_user))
    );
    assert!(
        accept_apub["object"]["object"]
            .as_str()
            .unwrap()
            .ends_with(&format!("/apub/users/{}", target_user))
    );

    let actor_apub = client
        .get(format!(
            "{}/apub/users/{}",
            server.real_host_url, target_user
        ))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(actor_apub["followers"].as_str().is_some());

    client
        .post(format!(
            "{}/api/unstable/users/{}/unfollow",
            server.real_host_url, target_user
        ))
        .bearer_auth(&follower_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    let follow_after_unfollow = client
        .get(format!(
            "{}/apub/users/{}/followers/{}",
            server.real_host_url, target_user, follower_user
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        follow_after_unfollow.status(),
        reqwest::StatusCode::NOT_FOUND
    );
}
