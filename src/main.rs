extern crate r2d2;
extern crate r2d2_sqlite;
extern crate rusqlite;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rumqttd::{Broker, Config, Notification};
use rusqlite::Connection;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::str;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio;
use std::future::Future;
use std::pin::Pin;

#[derive(Debug)]
struct Event {
    id: u32,
    r#type: String,
    data: Vec<u8>,
    time: u64,
}

fn is_user_exist(conn: &Connection, username: &str, password: &str) -> bool {
    let mut password_hasher = DefaultHasher::new();
    password.hash(&mut password_hasher);
    let mut stmt = conn
        .prepare("SELECT username FROM users WHERE username = ?1 AND password = ?2")
        .unwrap();
    let mut rows = stmt
        .query([username, &password_hasher.finish().to_string()])
        .unwrap();
    return rows.next().is_ok();
}

fn create_user(conn_pool: Pool<SqliteConnectionManager>, username: &str, password: &str) {
    let conn = conn_pool.get().unwrap();
    let mut password_hasher = DefaultHasher::new();
    password.hash(&mut password_hasher);
    let pass_hash = password_hasher.finish().to_string();
    let epoch_now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    conn.execute(
        "INSERT INTO users (username, password, created_time) VALUES (?1, ?2, ?3)",
        (username, pass_hash, epoch_now),
    )
    .unwrap_or_else(|e| {
        println!("Error creating user: {}", e);
        0 // Return value for number of rows affected
    });
}

fn add_event(conn_pool: Pool<SqliteConnectionManager>, r#type: &str, data: &[u8]) {
    let conn = conn_pool.get().unwrap();
    let epoch_now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    conn.execute(
        "INSERT INTO event (type, data, time) VALUES (?1, ?2, ?3)",
        (r#type, data, epoch_now),
    )
    .unwrap();
}

fn get_all_event(conn_pool: Pool<SqliteConnectionManager>) -> Vec<Event> {
    let conn = conn_pool.get().unwrap();
    let mut stmt = conn
        .prepare("SELECT id, type, data, time FROM event")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();

    let mut events: Vec<Event> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        events.push(Event {
            id: row.get(0).unwrap(),
            r#type: row.get(1).unwrap(),
            data: row.get(2).unwrap(),
            time: row.get(3).unwrap(),
        });
    }
    return events;
}

fn get_event(conn_pool: Pool<SqliteConnectionManager>, r#type: &str, count: u32) -> Vec<Event> {
    let conn = conn_pool.get().unwrap();
    let epoch_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let mut stmt = conn
        .prepare("SELECT id, type, data, time FROM event WHERE type = ?1 AND time <= ?2 LIMIT ?3")
        .unwrap();
    let mut rows = stmt
        .query([r#type, &epoch_now.to_string(), &count.to_string()])
        .unwrap();

    let mut events: Vec<Event> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        events.push(Event {
            id: row.get(0).unwrap(),
            r#type: row.get(1).unwrap(),
            data: row.get(2).unwrap(),
            time: row.get(3).unwrap(),
        });
    }
    return events;
}

fn init_db(conn_pool: Pool<SqliteConnectionManager>) {
    // SELECT name FROM sqlite_schema WHERE type='table' AND name='{table_name}';
    let conn = conn_pool.get().unwrap();
    conn.execute(
        "CREATE TABLE if not exists users (
            username    TEXT PRIMARY KEY,
            password  TEXT NOT NULL,
            created_time  INTEGER NOT NULL,
            updated_time  INTEGER
        )",
        (),
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE if not exists event (
            id    INTEGER PRIMARY KEY,
            type  TEXT NOT NULL,
            data  BLOB NOT NULL,
            time  INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();

    conn.execute(
        "CREATE TABLE if not exists event_tracker (
            id    INTEGER PRIMARY KEY,
            type  TEXT NOT NULL,
            data  BLOB NOT NULL,
            time  INTEGER NOT NULL
        )",
        (),
    )
    .unwrap();
}

fn auth(conn_pool: Pool<SqliteConnectionManager>) -> Box<dyn Fn(String, String, String) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send + Sync> {
    Box::new(move |_client_id, username, password| {
        let pool = conn_pool.clone();
        Box::pin(async move {
            let conn = pool.get().unwrap();
            is_user_exist(&conn, &username, &password)
        })
    })
}

#[tokio::main]
async fn main() {
    let manager = SqliteConnectionManager::file("./event.db");
    let pool = r2d2::Pool::new(manager).unwrap();
    init_db(pool.clone());
    // add_event(&conn, "event001", "AAAA".as_bytes());
    // add_event(&conn, "event002", "1111".as_bytes());
    // add_event(&conn, "event001", "BBBB".as_bytes());
    create_user(pool.clone(), "user01", "password1");
    create_user(pool.clone(), "user02", "password2");
    create_user(pool.clone(), "user03", "password3");
    let all_event = get_all_event(pool.clone());
    for event in all_event {
        println!(
            "get all event-Found event {} - {} - {} - {:?}",
            event.id, event.r#type, event.time, event.data
        );
    }

    let events = get_event(pool.clone(), "event001", 2);
    for event in events {
        println!(
            "get some event-Found event {} - {} - {} - {:?}",
            event.id, event.r#type, event.time, event.data
        );
    }

    let _test001 = (42, "the Answer");

    let buf = rmp_serde::to_vec(&(42, "the Answer")).unwrap();

    assert_eq!(
        vec![0x92, 0x2a, 0xaa, 0x74, 0x68, 0x65, 0x20, 0x41, 0x6e, 0x73, 0x77, 0x65, 0x72],
        buf
    );

    assert_eq!((42, "the Answer"), rmp_serde::from_slice(&buf).unwrap());

    let config = config::Config::builder()
        .add_source(config::File::with_name("rumqttd.toml"))
        .build()
        .unwrap();

    let mut config: Config = config.try_deserialize().unwrap();

    // for e.g. if you want it for [v4.1] server, you can do something like
    let server = config.v4.as_mut().and_then(|v4| v4.get_mut("1")).unwrap();
    let auth_function = auth(pool.clone());
    server.set_auth_handler(auth_function);

    // dbg!(&config);
    let mut broker = Broker::new(config);
    let (mut link_tx, mut link_rx) = broker.link("singlenode").unwrap();
    thread::spawn(move || {
        broker.start().unwrap();
    });

    link_tx.subscribe("#").unwrap();

    let mut count = 0;

    loop {
        let notification = match link_rx.recv().unwrap() {
            Some(v) => v,
            None => continue,
        };

        match notification {
            Notification::Forward(forward) => {
                count += 1;
                println!(
                    "Topic = {:?}, Count = {}, Len = {} bytes, Payload = {:?} ",
                    forward.publish.topic,
                    count,
                    forward.publish.payload.len(),
                    forward.publish.payload
                );
                const ADD_TOPIC_PREFIX: &[u8] = "/events/add/".as_bytes();
                if forward.publish.topic.starts_with(&ADD_TOPIC_PREFIX) {
                    let type_name_bytes = forward.publish.topic.slice(ADD_TOPIC_PREFIX.len()..);
                    let type_name = str::from_utf8(&type_name_bytes).unwrap();
                    println!("put event, type: {:?}", type_name);
                    let result: rmpv::Value =
                        rmp_serde::from_slice(&forward.publish.payload).unwrap();
                    let json = serde_json::to_string(&result).unwrap();
                    println!("json result : {}", json);
                    add_event(pool.clone(), type_name, &forward.publish.payload);
                }
            }
            v => {
                println!("{v:?}");
            }
        }
    }
}
