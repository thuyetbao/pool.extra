#' Is Pool
#'
#' @description validate pool object
#' @export
is_pool <- function(x) {
  inherits(x, what = c("Pool", "R6"))
}

#' Format Number
#'
#' @description format number in science
#' @export
format_number <- function(x, ...) {
  base::stopifnot(is.numeric(x))
  formatC(x, format = "f", digits = 0, big.mark = ",", ...)
}

#' Pool Driver
#'
#' @description get Connection Driver of pool
#' @export
pool_driver <- function(pool) {
  return(pool$.__enclos_env__$private$createObject())
}

#' Pool Version
#'
#' @description get version of database
#' @examples
#' pool_version(pool = lime)
#' @references
#' MySQL :: MySQL 8.0 Reference Manual :: 12.15 Information Functions
#' @export
pool_version <- function(pool) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool))

  #' Get
  .ver <- pool_query(
    pool,
    "SELECT VERSION()"
  ) %>% tidytable::pull.()

  #' Return
  return(.ver)
}

#' Pool Schema
#'
#' @description get schema working on
#' @export
pool_schema <- function(pool) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool))

  #' Get
  .sche <- pool_query(
    pool,
    "SELECT SCHEMA()"
  ) %>% tidytable::pull.()

  #' Return
  return(.sche)
}

#' Pool Dir
#'
#' @description ger all table by implement `DBI::dbReadTable()` function
#' @export
pool_dir <- function(pool) {
  .dir <- pool_query(pool, "SHOW VARIABLES WHERE Variable_Name LIKE '%dir'")
  return(.dir)
}

#' Pool Table
#'
#' @description ger all table by implement `DBI::dbReadTable()` function
#' @export
pool_table <- function(pool, table, .parse = tidytable::as_tidytable) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table), rlang::is_function(.parse))

  #' Get
  .tabu <- pool_query(
    pool,
    modify_sql(
      pool,
      "SELECT * FROM {`tabu`}",
      tabu = DBI::dbQuoteIdentifier(pool, table)
      )
  ) %>% .parse

  #' Return
  return(.tabu)
}

#' Pool Names
#'
#' @description get header of table in database
#' @export
pool_names <- function(pool, table) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table))

  #' Get
  nm <- DBI::dbListFields(pool, name = table)

  #' Return
  return(nm)
}

#' Pool Show
#'
#' @description show CREATE TABLE methodology
#' @export
pool_show <- function(pool, table) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table))

  #' Show
  show <- pool_query(
    pool,
    modify_sql(
      pool,
      "SHOW CREATE TABLE {`selector`}",
      selector = DBI::dbQuoteIdentifier(pool, table)
    )
  ) %>%
    tidytable::pull.(!!sym("Create Table"))

  #' Cat
  cat(cli::col_cyan(show))

  #' Return
  return(invisible(show))
}

#' Pool Describe
#'
#' @description used to get fields information in database
#' @note using for MySQL Database
#' @examples
#' pool_describe(lime, table = "branchs_intraday")
#' @export
pool_describe <- function(pool, table) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table))

  #' Postgres
  if (inherits(pool_driver(pool), "PqConnection")) {
    cli::cli_alert_warning("Still not implement for Postgres")
  }

  #' MariaDB
  if (inherits(pool_driver(pool), "MariaDBConnection")) {

    #' Get
    get <- pool_query(
      pool,
      modify_sql(
        pool,
        "DESCRIBE {`selector`}",
        selector = DBI::dbQuoteIdentifier(pool, table)
      )
    )

    #' Return
    return(get)
  }
}

#' Pool Show
#'
#' @description show the way to create TABLE
#' @examples
#' pool_call()
#' tables <- DBI::dbListTables(lime)
#' pool_show(lime, table = tables[1])
#' @export
pool_head <- function(pool, table, n = 5) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table), !rlang::is_missing(n), n >= 0)

  #' Head
  .tabu <- pool_query(
    pool,
    modify_sql(
      pool,
      "SELECT
      *
      FROM
      {`tabu`}
      LIMIT {row}",
      tabu = DBI::dbQuoteIdentifier(pool, table),
      row = n
    )
  )

  #' Return
  return(.tabu)
}

#' Pool Show
#'
#' @description show the way to create TABLE
#' @export
pool_add_column <- function(pool, table, column, type, not_null = FALSE, .before = NULL, .after = NULL) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(table), !rlang::is_missing(column),
    !rlang::is_missing(type), not_null %in% c(TRUE, FALSE), length(.before) <= 1,
    length(.after) <= 1, sum(length(.before), length(.after)) <= 1
  )

  #' Selector
  #' Null
  .null <- dplyr::case_when(
    base::isTRUE(not_null) ~ "NOT NULL",
    TRUE ~ ""
  )
  #' Position
  position <- dplyr::case_when(
    all(!is.null(.before), is.null(.after)) ~ paste("BEFORE", .before),
    all(is.null(.before), !is.null(.after)) ~ paste("AFTER", .after),
    TRUE ~ ""
  )

  #' Alter
  pool_execute(
    pool,
    modify_sql(
      pool,
      "ALTER TABLE {`tabu`}
      ADD COLUMN {col} {typ} {nul} {pos};",
      tabu = DBI::dbQuoteIdentifier(pool, table),
      col = DBI::SQL(column),
      typ = DBI::SQL(type),
      nul = DBI::SQL(.null),
      pos = DBI::SQL(position)
    )
  )
}

#' Pool Drop Column
#'
#' @description drop column
#' @export
pool_drop_column <- function(pool, table, column) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table), !rlang::is_missing(column))

  #' Drop Column
  pool_execute(
    pool,
    modify_sql(
      pool,
      "ALTER TABLE {tabu}
      DROP {column}",
      tabu = DBI::dbQuoteIdentifier(pool, table),
      column = DBI::dbQuoteIdentifier(pool, column)
    )
  )
}

#' Pool Constraint
#'
#' @description get constraint of table
#' @examples
#' pool_call()
#' pool_constraint(lime)
#' @export
pool_constraint <- function(pool, table = NULL) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool))

  #' Postgres
  if (inherits(pool_driver(pool), "PqConnection")) {
    info <- pool_query(
      pool,
      "SELECT
      kcu.table_schema,
      kcu.table_name,
      tco.constraint_name,
      tco.constraint_type,
      kcu.ordinal_position as position,
      kcu.column_name as column_name
      FROM
      information_schema.table_constraints tco
      JOIN information_schema.key_column_usage kcu
      ON kcu.constraint_name = tco.constraint_name
      and kcu.constraint_schema = tco.constraint_schema
      and kcu.constraint_name = tco.constraint_name
      ORDER BY
      kcu.table_schema, kcu.table_name, position;"
    )
  }

  #' MariaDB
  if (inherits(pool_driver(pool), "MariaDBConnection")) {
    info <- pool_query(
      pool,
      "SELECT
      TABLE_SCHEMA AS table_schema,
      TABLE_NAME AS table_name,
      CONSTRAINT_NAME AS constraint_name,
      REFERENCED_TABLE_NAME AS referenced_table_name,
      COLUMN_NAME AS column_name,
      REFERENCED_COLUMN_NAME AS referenced_column_name,
      CONSTRAINT_NAME AS constraint_name
      FROM
      INFORMATION_SCHEMA.KEY_COLUMN_USAGE
      WHERE REFERENCED_TABLE_SCHEMA IS NOT NULL;"
    )
  }

  #' Valid
  if (!is.null(table)) {
    info <- tidytable::filter.(info, !!sym("table_name") == !!table)
  }

  #' Return
  return(info)
}

#' Pool Drop Key
#'
#' @description drop key (foreign or primary) of a table in databsse
#' @export
pool_drop_key <- function(pool, table, key = c("primary", "foreign"), constraint = NULL) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(table),
    key %in% c("foreign", "primary"), length(key) == 1
  )

  #' Navigator
  if (key == "primary") {

    #' Excute
    pool_execute(
      pool,
      modify_sql(
        pool,
        "ALTER TABLE {`tab`}
        DROP PRIMARY KEY;",
        tab = DBI::dbQuoteIdentifier(pool, table)
      )
    )

    #' Message
    cli::cli_alert_success("Succcess drop primary key on table {cli::col_blue(table)}")
  } else if (key == "foreign") {

    #' Stop
    base::stopifnot(!is.null(column))

    #' Excute
    pool_execute(
      pool,
      modify_sql(
        pool,
        "ALTER TABLE {`tab`}
        DROP FOREIGN KEY {`col`};",
        tab = DBI::dbQuoteIdentifier(pool, table),
        col = constraint
      )
    )

    #' Message
    cli::cli_alert_success("Succcess drop foreign key at constraint {cli::col_cyan(constraint)} \\
                           on {cli::col_blue(table)}")
  }
}

#' Pool Drop Table
#'
#' @description drop table databsse
#' @export
pool_drop_table <- function(pool, table, .error = TRUE, .message = TRUE) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(table),
    .error %in% c(TRUE, FALSE), .message %in% c(TRUE, FALSE)
  )

  #' Table Information
  existance_status <- DBI::dbExistsTable(pool, table)

  #' Navigator
  if (existance_status) {

    #' Information
    rows <- pool_nrow(pool, table)
    cols <- pool_ncol(pool, table)
    size <- pool_size(pool, table, size_of = "table", unit = "MB")

    #' Information
    cli::cli_alert(paste(
      "Are you sure to delete `{table}` table with below information\n",
      "Total observations: {rows*cols} obs ({cols} columns * {rows} rows)\n",
      "Size (MB) of table: {format(size, big.mark = ',', digits = 2)} MB\n"
    ))

    #' Confirm
    confirm <- base::readline(prompt = "Choose Yes/No? ")

    #' Navigator
    if (identical(stringr::str_to_lower(confirm), "yes")) {

      #' Error Handling
      error <- dplyr::case_when(
        base::isTRUE(.error) ~ "IF EXISTS",
        TRUE ~ ""
      )

      #' Excute
      pool_execute(
        pool,
        modify_sql(
          pool,
          "DROP TABLE {err} {`tab`};",
          err = DBI::SQL(error),
          tab = DBI::dbQuoteIdentifier(pool, table)
        )
      )

      #' Message
      if (!base::isTRUE(DBI::dbExistsTable(pool, table))) {
        cli::cli_alert_success("Succcess drop table `{cli::col_blue(table)}`")
      } else {
        cli::cli_alert_danger("Process to delete `{cli::col_blue(table)}` has errors!")
      }
    } else {

      #' Message
      cli::cli_alert_info("Exit process to delete `{cli::col_blue(table)}` table")
    }
  } else if (all(!base::isTRUE(existance_status), base::isTRUE(.message))) {
    #' Message
    cli::cli_alert_info("Table `{cli::col_blue(table)}` not exists in database")
  }
}

#' Pool Size
#'
#' @description drop key (foreign or primary) of a table in databsse
#' @references
#' \href{Size of table database}{https://www.a2hosting.com/kb/developer-corner/mysql/determining-the-size-of-mysql-databases-and-tables}
#' @export
pool_size <- function(pool, name, size_of = c("table", "schema"), unit = "MB") {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(name),
    size_of %in% c("table", "schema"), unit %in% c("MB")
  )

  #' Selector
  size_of_selector <- ifelse(length(size_of) != 1, size_of[1], size_of)

  #' Size
  master <- pool_query(
    pool,
    modify_sql(
      pool,
      "SELECT
      table_schema,
      table_name,
      ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS size
      FROM information_schema.TABLES
      GROUP BY table_schema, table_name;"
    )
  )

  #' Navigator
  if (size_of == "table") {

    #' Stop
    base::stopifnot(name %in% base::unique(master$table_name))

    #' Calculate
    size <- master %>%
      tidytable::filter.(!!sym("table_name") == name) %>%
      tidytable::select.(!!sym("size")) %>%
      tidytable::pull.()
  } else if (size_of == "schema") {

    #' Stop
    base::stopifnot(name %in% base::unique(master$table_schema))

    #' Calculate
    size <- master %>%
      tidytable::filter.(!!sym("table_schema") == name) %>%
      tidytable::select.(!!sym("size")) %>%
      tidytable::pull.()
  }

  #' Parse
  parse_size <- as.numeric(size)

  #' Return
  return(parse_size)
}

#' Pool Nrow
#'
#' @description count number of rows in specific table
#' @examples
#' pool_call()
#' tables <- DBI::dbListTables(lime)
#' pool_nrow(lime, table = tables[1])
#' @export
pool_nrow <- function(pool, table) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table))

  #' Head
  ro <- pool_query(
    pool,
    modify_sql(
      pool,
      "SELECT
      COALESCE(COUNT(*), 0)
      FROM
      {`selector`}",
      selector = DBI::dbQuoteIdentifier(pool, table)
    )
  ) %>%
    tidytable::pull.()

  #' Return
  return(ro)
}

#' Pool Number of Column
#'
#' @description count number of rows in specific table
#' @export
pool_ncol <- function(pool, table) {

  #' Stop
  base::stopifnot(!rlang::is_missing(pool), !rlang::is_missing(table))

  #' Column
  table_chaining <- pool_describe(pool, table)

  #' Counter
  cols <- nrow(table_chaining)

  #' Return
  return(cols)
}

#' Pool Index
#'
#' @description count number of rows in specific table
#' @export
pool_index <- function() {
  #' https://www.w3schools.com/sql/sql_create_index.asp
}

#' Pool Index
#'
#' @description count number of rows in specific table
#' @export
pool_drop_index <- function() {
  #' https://www.w3schools.com/sql/sql_create_index.asp
}

#' Pool Grant
#'
#' @description grant privilege for user
#' @param privilege privilege of user in objects from pool user
#' @param object name of objects in database, included: TABLE, VIEW, STORED PROC and SEQUENCE.
#' @param grant_options WITH_GRANT OPTIONS (default FALSE) give permissions for other users from user
#' @references
#' \href{MYSQL Privilege}{https://dev.mysql.com/doc/refman/8.0/en/grant.html}
#' \href{}{https://thuthuat.taimienphi.vn/grant-revoke-trong-sql-33413n.aspx}
#' @export
pool_grant <- function(pool, privilege, object, user, grant_options = FALSE) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(privilege),
    !rlang::is_missing(object), !rlang::is_missing(user)
  )

  #' Options
  if (base::isFALSE(options)) {
    .options <- glue::glue("")
  } else {
    .options <- glue::glue(" WITH GRANT OPTION;")
  }

  #' Grant
  pool_execute(
    pool,
    modify_sql(
      pool,
      "GRANT {DBI::SQL(privilege)} ON {obj} TO {user} {DBI::SQL(.options)};",
      obj = DBI::dbQuoteIdentifier(pool, object)
    )
  )
}

#' Pool Table Search
#'
#' @description get relevants table based on regrex component
#' @export
pool_search <- function(pool, pattern) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(pattern)
  )

  #' Get
  .tables <- DBI::dbListTables(pool)

  #' Regrex
  .regrex <- base::grep(pattern = pattern, x = .tables, ignore.case = FALSE)

  #' Parsing
  if (length(.regrex) != 0) {
    .table_search <- .tables[.regrex]
  } else {
    .table_search <- NA_character_
  }

  #' Return
  return(.table_search)
}

#' Pool Database List
#'
#' @description get table in schema
#' @export
pool_db_list <- function(pool, schema = NULL) {

  #' Stop
  stopifnot(!rlang::is_missing(pool))

  #' Valid
  if (is.null(schema)) {
    .db <- DBI::dbListTables(pool)
  } else {
    .db <- pool_query(
      pool,
      modify_sql(
        pool,
        "SELECT
        DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = {sc}",
        sc = schema
      )
    ) %>% tidytable::pull.()
  }

  #' Return
  return(.db)
}
