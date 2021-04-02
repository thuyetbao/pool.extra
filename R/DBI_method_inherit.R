#' DBI Method Inherit for pool

#' @importMethodsFrom DBI dbQuoteLiteral dbDisconnect dbListFields
#' @importFrom pool poolCheckout poolReturn

#' dbQuoteLiteral
#'
#' @details
#' As of 2020-05-07, this is necessary due to an incompatiblity
#' between packages `pool` (v 0.1.4.3) and `glue` (v >= 1.3.2).
#' In v1.3.2, `glue` started using `dbQuoteLiteral`, which is
#' currently not offered by `pool`, so creating it here.
#' @export
setMethod(
  "dbQuoteLiteral", c("Pool", "ANY"),
  function(conn, x, ...) {
    connection <- pool::poolCheckout(conn)
    on.exit(pool::poolReturn(connection))
    DBI::dbQuoteLiteral(connection, x, ...)
  }
)

#' dbDisconnect
#'
#' @references
#' Follow S4 Method:
#' \url{https://stackoverflow.com/questions/46475787/s4-setting-method-errors-no-existing-definition-of-function}
#' @export
setMethod(
  "dbDisconnect", c("Pool"),
  function(conn, ...) {
    connection <- pool::poolCheckout(conn)
    on.exit(pool::poolReturn(connection))
    DBI::dbDisconnect(connection, ...)
  }
)


#' dbListFields
#'
#' @importMethodsFrom DBI 
#' @importFrom pool poolCheckout poolReturn
#' @export
setMethod(
  "dbListFields", c("Pool", "ANY"),
  function(conn, name, ...) {
    connection <- pool::poolCheckout(conn)
    on.exit(pool::poolReturn(connection))
    DBI::dbListFields(connection, name, ...)
  }
)
