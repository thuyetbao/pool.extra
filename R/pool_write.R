#' Pool Write
#'
#' @description write information to database
#' @export
pool_write <- function(pool, table, df, .batch = numeric(0), row.names = FALSE, temporary = FALSE, overwrite = FALSE, append = TRUE) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(df), !rlang::is_missing(table), is.numeric(.batch)
  )

  #' Valid Data Type
  if (!inherits(df, "data.frame")) {
    df <- as.data.frame(df, row.names = FALSE)
    if (!inherits(df, "data.frame")) {
      stop("Can't parse `df` to data.frame")
    }
  }
  
  #' Batch Control
  if(length(.batch) == 0) {
    split <- nrow(df)
    .batch <- 1
  } else if (.batch > 0) {
    split <- ceiling(nrow(df) / .batch)
  } else if (.batch < 0) {
    stop("Batch number can't be negative number")
  }

  #' Batch
  for (incre in 1:.batch) {
    
    #' Write
    DBI::dbWriteTable(
      conn = pool,
      name = DBI::dbQuoteIdentifier(conn = pool, x = table),
      value = as.data.frame(tidytable::slice.(df, .split_sequence(incre, split))),
      row.names = row.names,
      temporary = temporary,
      overwrite = overwrite,
      append = append
    )
  }
}

#' Split Sequence 
.split_sequence <- function(.increase, size) {
  
  #' Stop 
  base::stopifnot(
    !rlang::is_missing(.increase), !rlang::is_missing(size)
  )
  
  #' Count
  counter <- ((.increase - 1) * size + 1):((.increase - 1) * size + size)
  
  #' Return
  return(counter)
}
