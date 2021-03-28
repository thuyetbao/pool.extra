#' Pool Query
#'
#' @description method of query to database managed by pool
#' @param pool pool object, created by `pool::dbPool`
#' @param statement SQL statement send to database
#' @param ... Other parameters passed on to methods align with `DBI` package
#' @export
pool_query <- function(pool,
                       statement,
                       .parse = TRUE,
                       .parse_funs = tidytable::as_tidytable,
                       ...) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(statement))

  #' Checkout
  conn <- pool::poolCheckout(pool)
  on.exit(expr = pool::poolReturn(conn))

  #' Send
  query <- DBI::dbSendQuery(conn, statement, ...)

  #' Fetch
  res <- DBI::dbFetch(query)

  #' Parsing
  if (.parse) {
    res <- .parse_funs(res)
  }

  #' Clear
  DBI::dbClearResult(query)

  #' Return
  return(res)
}

#' Pool Insert
#'
#' @description method of excute statment to database managed by pool
#'
#' @param pool pool object, created by `pool::dbPool`
#' @param statement SQL statement send to database
#' @param ... Other parameters passed on to method, align with `DBI` package
#'
#' @references
#' \href{SQL Specifiation}{https://cran.r-project.org/web/packages/DBI/vignettes/spec.html}
#' \href{Issue 88 of Pool}{https://github.com/rstudio/pool/issues/88}
#' @export
pool_execute <- function(pool,
                         statement,
                         ...) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(statement))

  #' Checkout
  conn <- pool::poolCheckout(pool)
  on.exit(pool::poolReturn(conn))

  #' Send
  res <- DBI::dbSendStatement(conn, statement, ...)

  #' Row Affect
  affect <- DBI::dbGetRowsAffected(res)

  #' Clear
  DBI::dbClearResult(res)

  #' Return
  return(affect)
}
