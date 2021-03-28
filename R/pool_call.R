#' Pool Call
#'
#' @description database connector using pool
#' @details
#' Pool Call fundamentals:
#' 1. Pool search path in working directory any .yml file if can't see file in path
#' 2. Pool if not define then will match all names in config file
#' 3. Error detect Session with more message in global view
#'
#' @param pool [character] pool names, define related in configuration file (which has suffix of `*.yml`)
#' @param path [relative path] path to configuration file, using `config` packages
#' @param timeout [numeric] number timeout in seconds, default with 7200000
#' @param .message [boolean] appear message when successful connect to database
#' @param .search_parent [boolean] default TRUE, config will search config file in parent folder in case not see file in `path` parameter
#'
#' @note
#' For config file setup. It need to follow this pattern and save with `*.yml` extension.
#' demo.yml
#' ---
#' default:
#'   <Pool-Name>:
#'      driver: "Postgres"
#'      db_host: ""
#'      db_name: "database-name"
#'      db_port: "5432"
#'      user: ""
#'      pass: ""
#'
#' ---
#' For driver: database driver, such as MariaDB `RMariaDB::MariaDB()` or Postgres `RPostgres::Postgres()`
#' Remind that this file is sensitive with end of line.
#'
#' @example
#' pool_call()
#' @export
pool_call <- function(pool = NULL,
                      path = file.path("config.yml"),
                      timeout = 7200000,
                      envir = rlang::global_env(),
                      .message = TRUE,
                      .search_parent = TRUE) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(path))

  #' Null pool
  if (is.null(pool)) {
    pool <- names(config::get(file = path, use_parent = .search_parent))
  }

  #' Character Valid
  if (!is.character(pool)) {
    stop("`pool` can't be class of `", class(pool), "`. References character name of pool.")
  }

  #' Loop
  for (i in 1:length(pool)) {

    #' Get
    pol <- pool[i]

    #' Configuration
    .conf <- config::get(value = pol, file = path, use_parent = .search_parent)

    #' Valid
    if (is.null(.conf)) {
      stop("Pool `", pol, "` not store any value.")
    }

    #' Driver
    dri <- .conf$driver

    #' Valid
    if (is.null(dri) | is.na(dri) | length(dri) == 0) {
      stop("Pool `", pol, "` not declare driver information. Please refer database client driver in `config` file.")
    }

    #' Parse
    if (tolower(dri) == "postgres") {
      driv <- RPostgres::Postgres()
    } else if (tolower(dri) == "mariadb" | tolower(dri) == "mysql") {
      driv <- RMariaDB::MariaDB()
    }

    #' Assign
    base::assign(
      x = pol,
      value = pool::dbPool(
        drv = driv,
        host = .conf$db_host,
        port = .conf$db_port,
        dbname = .conf$db_name,
        user = .conf$user,
        password = .conf$pass,
        minSize = 1,
        maxSize = Inf,
        idleTimeout = timeout
      ), envir = envir
    )

    #' Mes
    if (.message) {
      cli::cli_alert_success("Connect to database using {crayon::bgBlack(cli::col_white(pol))} pool")
    }
  }
}
