#' dbQuoteLiteral
#'
#' @details
#' As of 2020-05-07, this is necessary due to an incompatiblity
#' between packages `pool` (v 0.1.4.3) and `glue` (v >= 1.3.2).
#' In v1.3.2, `glue` started using `dbQuoteLiteral`, which is
#' currently not offered by `pool`, so creating it here.
#' @importMethodsFrom DBI dbQuoteLiteral
#' @importFrom pool poolCheckout poolReturn
#' @export
setMethod(
  "dbQuoteLiteral", c("Pool", "ANY"),
  function(conn, x, ...) {
    connection <- pool::poolCheckout(conn)
    on.exit(pool::poolReturn(connection))
    DBI::dbQuoteLiteral(connection, x, ...)
  }
)
