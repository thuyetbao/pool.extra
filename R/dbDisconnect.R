#' dbDisconnect
#'
#' @details
#' @importMethodsFrom DBI dbDisconnect
#' @importFrom pool poolCheckout poolReturn
#' @export
setMethod(
  "dbDisconnect", c("Pool"),
  function(conn, ...) {
    connection <- pool::poolCheckout(conn)
    on.exit(pool::poolReturn(connection))
    DBI::dbDisconnect(connection, ...)
  }
)
