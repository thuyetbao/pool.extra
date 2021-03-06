% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/pool_call.R
\name{pool_call}
\alias{pool_call}
\title{Pool Call}
\usage{
pool_call(
  pool = NULL,
  path = file.path("config.yml"),
  timeout = 7200000,
  envir = rlang::global_env(),
  .message = TRUE,
  .search_parent = TRUE
)
}
\arguments{
\item{pool}{[character] pool names, define related in configuration file (which has suffix of `*.yml`)}

\item{path}{[relative path] path to configuration file, using `config` packages}

\item{timeout}{[numeric] number timeout in seconds, default with 7200000}

\item{.message}{[boolean] appear message when successful connect to database}

\item{.search_parent}{[boolean] default TRUE, config will search config file in parent folder in case not see file in `path` parameter}
}
\description{
database connector using pool
}
\details{
Pool Call fundamentals:
1. Pool search path in working directory any .yml file if can't see file in path
2. Pool if not define then will match all names in config file
3. Error detect Session with more message in global view
}
\note{
For config file setup. It need to follow this pattern and save with `*.yml` extension.
demo.yml
---
default:
  <Pool-Name>:
     driver: "Postgres"
     db_host: ""
     db_name: "database-name"
     db_port: "5432"
     user: ""
     pass: ""

---
For driver: database driver, such as MariaDB `RMariaDB::MariaDB()` or Postgres `RPostgres::Postgres()`
Remind that this file is sensitive with end of line.
}
