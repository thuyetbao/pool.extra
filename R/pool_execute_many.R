#' Execute Many
#'
#' @description implement execute many information by using only one execute
#' @references
#' \href{Optimizing Insert}{https://dev.mysql.com/doc/refman/5.7/en/insert-optimization.html#:~:text=You%20can%20use%20the%20following,separate%20single%2Drow%20INSERT%20statements.}
#' \href{Load Data MySQL}{https://dev.mysql.com/doc/refman/5.7/en/load-data.html}
#' @export
pool_execute_many <- function(pool, statement, data, ...) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(data), !rlang::is_missing(statement), is.data.frame(data)
  )

  #' Mapper
  .dot <- rlang::list2(...)

  #' Valid
  if (all(length(.dot) != 1, is.character(.dot[[1]]))) {
    stop(names(.dot), " can't has length greater than 1. Inorge `...` argument")
  }

  #' Assign
  base::assign(
    x = names(.dot),
    value = glue::glue_data_sql(.con = pool, data, .dot[[1]]) %>%
      glue::glue_collapse(sep = ", ") %>%
      DBI::SQL(),
    envir = base::sys.frame(which = 1)
  )

  #' Execute
  pool_execute(
    pool,
    modify_sql(
      pool,
      statement
    )
  )
}
