#' Modify SQL
#'
#' @description build-in function to manage SQL statement for pool method with binding fields features
#' @export
modify_sql <- function(pool, statement, ...) {

  #' Arguments
  parameters <- rlang::enquos(...,
    .named = FALSE, .ignore_empty = "all",
    .homonyms = "error", .check_assign = TRUE
  )

  #' Named Quosures and Parse Parameter
  names(parameters) %>% purrr::map(
    ~ base::assign(.,
                   value = rlang::eval_tidy(parameters[[.]]),
                   pos = base::sys.frame(which = 1))
    )

  #' Builder
  sql <- glue::glue_sql(.con = pool, statement, .envir = base::sys.frame(which = 1))

  #' Return
  return(sql)
}
