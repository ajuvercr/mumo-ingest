use std::sync::Mutex;

use actix_web::{get, guard, post, web, App, HttpResponse, HttpServer, Responder};
use files::State;
use serde::Deserialize;

mod files;

type St = Mutex<State>;

#[derive(Deserialize)]
struct Info {
    index: u64,
}

#[get("/")]
async fn hello(data: web::Data<St>, info: web::Query<Info>) -> impl Responder {
    if let Ok(mut st) = data.lock() {
        if let Ok(data) = st.read(info.index) {
            HttpResponse::Ok().body(data)
        } else {
            HttpResponse::NotFound().finish()
        }
    } else {
        HttpResponse::InternalServerError().body("lock failed")
    }
}

#[post("/")]
async fn echo(data: web::Data<St>, req_body: web::Bytes) -> impl Responder {
    if let Ok(mut st) = data.lock() {
        if let Ok(_) = st.write(&req_body) {
            HttpResponse::Ok().finish()
        } else {
            HttpResponse::NotFound().finish()
        }
    } else {
        HttpResponse::InternalServerError().body("lock failed")
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Note: web::Data created _outside_ HttpServer::new closure
    let state = web::Data::new(Mutex::new(State::new("data/data.bin", "data/indices.bin")?));

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone()) // <- register the created data
            .service(
                web::scope("/priv")
                    .guard(guard::fn_guard(|ctx| {
                        if let Some(query) = ctx.head().uri.query() {
                            let params = querystring::querify(query);
                            params.iter().any(|(k, v)| *k == "key" && *v == "abc")
                        } else {
                            false
                        }
                    }))
                    .service(hello),
            )
            .service(hello)
            .service(echo)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
