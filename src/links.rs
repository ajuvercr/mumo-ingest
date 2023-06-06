use std::{collections::HashMap, fmt::Display};

use actix_web::http::{
    uri::{Parts, PathAndQuery},
    Uri,
};

pub struct Link {
    current: u64,
    last: u64,
}

enum Kind {
    Prev,
    Next,
    Last,
    First,
}

impl Kind {
    fn apply(&self, idx: u64, max: u64) -> Option<u64> {
        match (self, idx) {
            (Kind::Prev, 0) => None,
            (Kind::Prev, x) => Some(x - 1),
            (Kind::Next, x) => (x + 1 < max).then_some(x + 1),
            (Kind::Last, _) => Some(max),
            (Kind::First, _) => Some(0),
        }
    }
}

impl Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Kind::Prev => write!(f, "prev"),
            Kind::Next => write!(f, "next"),
            Kind::Last => write!(f, "last"),
            Kind::First => write!(f, "first"),
        }
    }
}

fn clone_parts(parts: &Parts) -> Parts {
    let mut out = Parts::default();
    out.scheme = parts.scheme.clone();
    out.authority = parts.authority.clone();
    out.path_and_query = parts.path_and_query.clone();

    out
}

fn link_to<'a>(
    parts: &Parts,
    path: &str,
    query: &mut HashMap<&'a str, String>,
    idx: u64,
) -> Option<Uri> {
    let mut parts = clone_parts(parts);

    query.insert("index", idx.to_string());
    let mut query = querystring::stringify(query.iter().map(|x| (*x.0, x.1.as_str())).collect());
    query.pop(); // remove trailing & 
    parts.path_and_query = PathAndQuery::try_from(format!("{}?{}", path, query)).ok();

    Uri::from_parts(parts).ok()
}

impl Link {
    pub fn new(idx: u64, last: u64) -> Self {
        Self { current: idx, last }
    }

    pub fn header(&self, rel: &Uri) -> Option<String> {
        let parts = rel.clone().into_parts();
        let binding = parts.path_and_query.as_ref()?;
        let path = binding.path();
        let query_st = binding.query().unwrap_or("");

        let mut out = String::new();

        let mut query: HashMap<&'_ str, String> = querystring::querify(&query_st)
            .into_iter()
            .map(|(x, y)| (x, y.to_string()))
            .collect();
        let mut first = true;

        [Kind::Next, Kind::Prev, Kind::Last, Kind::First]
            .into_iter()
            .filter_map(|kind| {
                let link = kind.apply(self.current, self.last)?;
                Some(format!(
                    "<{}>; rel=\"{}\"",
                    link_to(&parts, path, &mut query, link)?,
                    kind
                ))
            })
            .for_each(|st| {
                // Intersperse when?
                if !first {
                    out += ", ";
                } else {
                    first = false;
                }

                out += &st;
            });

        (!first).then_some(out)
    }
}
