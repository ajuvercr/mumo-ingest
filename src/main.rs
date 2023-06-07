#[macro_use]
extern crate log;

use actix_web::{
    get,
    guard::{Guard, GuardContext},
    http::Uri,
    post,
    web::{self, Bytes, Payload},
    App, HttpResponse, HttpServer, Responder,
};
use files::State;
use serde::Deserialize;
use std::{borrow::Cow, path::PathBuf, sync::Mutex};

use crate::links::Link;

mod files;
mod links;

const HELP: &str = "\
Mumo-ingest

USAGE:
  mumo-ingest [OPTIONS] [INPUT]

FLAGS:
  -h, --help            Prints help information

OPTIONS:
  -p NUMBER             Set used port (default 8000)
  -d STRING             Path to store incoming data (default \"data/data.bin\")
  -i STRING             Path to store indices (default \"data/indices.bin\")
  -s STRING             Secret required as query param (default None)

ARGS:
  <HOST>                Host used, default 127.0.0.1
";

#[derive(Debug)]
struct AppArgs {
    port: u16,
    host: String,
    data: std::path::PathBuf,
    indices: std::path::PathBuf,
    secret: Option<String>,
}

fn parse_args() -> Result<AppArgs, pico_args::Error> {
    let mut pargs = pico_args::Arguments::from_env();

    // Help has a higher priority and should be handled separately.
    if pargs.contains(["-h", "--help"]) {
        print!("{}", HELP);
        std::process::exit(0);
    }

    let args = AppArgs {
        port: pargs.opt_value_from_str("-p")?.unwrap_or(8000),
        data: pargs
            .opt_value_from_str("-d")?
            .unwrap_or(PathBuf::from("data/data.bin")),
        indices: pargs
            .opt_value_from_str("-i")?
            .unwrap_or(PathBuf::from("data/indices.bin")),
        secret: pargs.opt_value_from_str("-s")?,
        // Parses a required free-standing/positional argument.
        host: pargs
            .opt_free_from_str()?
            .unwrap_or(String::from("127.0.0.1")),
    };

    // It's up to the caller what to do with the remaining arguments.
    let remaining = pargs.finish();

    if !remaining.is_empty() {
        eprintln!("Warning: unused arguments left: {:?}.", remaining);
    }

    Ok(args)
}

type St = Mutex<State>;

#[derive(Deserialize)]
struct Info {
    index: u64,
}

#[get("")]
async fn read_msg(data: web::Data<St>, info: web::Query<Info>, uri: Uri) -> impl Responder {
    trace!("handling uri {}", uri);
    if let Ok(mut st) = data.lock() {
        if let Ok((data, idx)) = st.read(info.index) {
            if let Some(header) = Link::new(idx.index, st.last()).header(&uri) {
                HttpResponse::Ok()
                    .insert_header(("Link", header))
                    .body(data)
            } else {
                HttpResponse::Ok().body(data)
            }
        } else {
            HttpResponse::NotFound().finish()
        }
    } else {
        HttpResponse::InternalServerError().body("lock failed")
    }
}

const MAX_SIZE: usize = 262_144_000; // max payload size is 256k
async fn extract_payload(mut payload: Payload) -> Result<Bytes, actix_web::Error> {
    use futures::StreamExt;

    let mut body = web::BytesMut::new();
    while let Some(chunk) = payload.next().await {
        trace!("chunk!");
        let chunk = chunk?;
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(actix_web::error::ErrorBadRequest("overflow"));
        }
        body.extend_from_slice(&chunk);
    }

    Ok(body.into())
}

#[post("")]
async fn write_msg(data: web::Data<St>, req_body: Payload) -> impl Responder {
    let bin = match extract_payload(req_body).await {
        Ok(x) => x,
        Err(e) => {
            error!("Acquire lock failed! {}", e);
            return HttpResponse::BadRequest().body("extracting payload failed");
        }
    };

    match data.lock() {
        Ok(mut st) => match st.write(&bin, true) {
            Ok(written) => HttpResponse::Ok().body(serde_json::to_string_pretty(&written).unwrap()),
            Err(e) => {
                warn!(err = log::as_error!(e); "Writing msg failed");
                HttpResponse::NotFound().finish()
            }
        },
        Err(e) => {
            error!("Acquire lock failed! {}", e);
            HttpResponse::InternalServerError().body("lock failed")
        }
    }
}

#[derive(Clone)]
struct QueryGuard {
    value: Option<Cow<'static, str>>,
}

impl Guard for QueryGuard {
    fn check(&self, ctx: &GuardContext<'_>) -> bool {
        match (self.value.as_ref(), ctx.head().uri.query()) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(sec), Some(query)) => {
                let params = querystring::querify(query);
                params.iter().any(|(k, v)| *k == "key" && *v == sec)
            }
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    pretty_env_logger::init();

    let AppArgs {
        port,
        host,
        data,
        secret,
        indices,
    } = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}.", e);
            std::process::exit(1);
        }
    };
    // Note: web::Data created _outside_ HttpServer::new closure
    let state = web::Data::new(Mutex::new(State::new(data, indices)?));
    if secret.is_some() {
        println!("Starting server on {}:{} with a secret", host, port);
    } else {
        println!("Starting server on {}:{} with no secrect", host, port);
    }
    let guard = QueryGuard {
        value: secret.map(Cow::Owned),
    };

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone()) // <- register the created data
            .service(
                web::scope("/")
                    .guard(guard.clone())
                    .service(read_msg)
                    .service(write_msg),
            )
    })
    .bind((host, port))?
    .run()
    .await
}
