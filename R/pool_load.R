#' Pool Load
#'
#' @description implement LOAD DATA INFILE of MySQL with pool management
#' @details
#' # Benefit
#' LOAD DATA INFILE has various benefits and is one of alternative way to upload data into database:
#' 1. Enhancement upload time, can be more than >100x to INSERT
#' 2. Use background server so that it' not harm another services that working on, such as R-session
#'
#' The reason behinds of LOAD DATA INFILE that make it more powerful and very fast because:
#' 1. There is no parsing of SQL data is read in big blocks.
#'
#' # Short-fall of LOAD DATA comes:
#'
#' 1. Rely heavily on System Infrastructure.
#' 2. Various control FILE both for server and database config.
#' 3. Securities problem details in \url{https://dev.mysql.com/doc/refman/8.0/en/load-data-local-security.html}
#'
#' # Technical Implementation:
#' 1. Configuration
#' a) For Database
#' - Assign FILE privileges for user that upload
#' ---
#' # Perform the following command as a privileged user (root).
#' # ----
#' # GRANT FILE ON *.* TO user@localhost;
#' # An alternative way of performing the same operation involves updating one of the MySQL server tables as follows:
#' # UPDATE mysql.user SET File_priv = 'Y' WHERE Host = 'localhost' AND User = 'user'; FLUSH PRIVILEGES;
#' ---
#' b) For System Folder
#' File Access for file or folder that put the file in
#' ---
#' Super User need to assign mod 666 or 777 to folder storage file with -r flags
#' ---
#'
#' 1. Transfer file to `/tmp/` folder that any user can access, for database user too
#' 2. Need validate objects with the target table in database
#' 3. Execute LOAD DATA INFINE with local arguments is TRUE
#'
#' # Data Types
#'
#' An empty field value is interpreted different from a missing field:
#'
#' For string types, the column is set to the empty string.
#'
#' For numeric types, the column is set to 0.
#'
#' For date and time types, the column is set to the appropriate “zero” value for the type. See Section 11.2, “Date and Time Data Types”.
#'
#' # Server Option Description
#' innodb_buffer_pool_size	Increase this if you have many indexes in InnoDB/XtraDB tables
#' key_buffer_size	Increase this if you have many indexes in MyISAM tables
#' max_allowed_packet	Increase this to allow bigger multi-insert statements
#' read_buffer_size	Read block size when reading a file with LOAD DATA
#' The access denied error could mean that:
#'
#' # Common Error
#' 1. FILE privileges
#' 'user'@'localhost' does not have the FILE privilege (GRANT FILE on *.* to user@'localhost'); or,
#' the file you are trying to load does not exist on the machine running mysql server (if using LOAD DATA INFILE); or,
#' the file you are trying to load does not exist on your local machine (if using LOAD DATA LOCAL INFILE); or,
#' the file you are trying to load is not world readable (you need the file and all parent directories to be world-readable: chmod 755 directory; and, chmod 744 file.dat)
#'
#' #' Err 1: Error executing query: Can't get stat of '/var/lib/mysql/phutoan_api/data/component.csv' (Errcode: 13)
#' #' Err 2:  Error: Error executing query: File 'phutoan_api/data/component.csv' not found (OS errno 2 - No such file or directory)
#'
#' pool_execute(lime, "SET GLOBAL local_infile=ON;")
#' The fastest way to load lots of data is using LOAD DATA INFILE.
#'
#' 2. Man in the middle
#' However, using "LOAD DATA LOCAL INFILE" (ie: loading a file from the client) may be a security problem :
#' A "man in the middle" proxy server can change the actual file requested from the server so the client will send a local file to this proxy.
#' if someone can execute a query from the client, he can have access to any file on the client (according to the rights of the user running the client process).
#' A specific option "allowLocalInfile" (default to true) can deactivate functionality on the client side. The global variable local_infile can disable LOAD DATA LOCAL INFILE on the server side.
#'
#' @param pool [environment] pool connection that summon by `pool_call()`
#' @param df [object] any objects but prefers to data.frame, object that not has `data.frame` class needed to parse back data.frame
#' @param capsule [path] operation system folder path that will transfer file to, default is temporary folder in `/tmp/` directly in folder
#' You can specify different folder path but make sure database user can access that folder to get file
#' @param .duplicate [character] default to `replace`, handling duplicate-key records, choose one of `replace` or `ignore`
#' @param table [character] table that transfer data
#' @param column [character] column, default is NULL that will push all columns of data.frame
#' @param local_load [boolean] default is TRUE, using local specific folder to the root server
#' @param clean_afterward [boolean] default is TRUE, delete folder that will transfer data to not remain garbage on server
#' @param auto_relocate [boolean] default is TRUE, auto-relocate columns positions in dataframe to match with columns in database.
#' @note
#' 1. Version support of MySQL is greater than 3.x
#' @references
#' \url{https://dev.mysql.com/doc/refman/8.0/en/load-data.html}
#' \url{http://www.it-iss.com/mysql/mysql-load-data-infile/}
#' \url{https://askubuntu.com/questions/1078366/how-to-execute-mysql-commands-in-sh}
#' \url{https://stackoverflow.com/questions/2221335/access-denied-for-load-data-infile-in-mysql}
#' \url{https://stackoverflow.com/questions/22859409/error-13-hy000-at-line-1-cant-get-stat-of-errcode-13}
#' \url{https://bbs.archlinux.org/viewtopic.php?id=227181}
#' \url{https://dev.mysql.com/doc/refman/8.0/en/load-data-local-security.html}
#' \url{https://mariadb.com/kb/en/about-mariadb-connector-j/}
#' \url{https://mariadb.com/kb/en/how-to-quickly-insert-data-into-mariadb/s}
#' \url{https://unix.stackexchange.com/questions/278184/copy-file-from-user-to-another-in-linux}
#' @export
pool_load <- function(pool,
                      df,
                      capsule = base::tempdir(check = FALSE),
                      delim = "\t", .enclosed = '"', .terminated = "\n", header = TRUE, .duplicate = "replace",
                      table, column = NULL,
                      user = NULL,
                      .user_config = file.path("config.yml"),
                      destination = Sys.getenv("DATABASE_DIRECTORY"),
                      local = TRUE,
                      load_local = TRUE,
                      clean_afterward = TRUE,
                      auto_relocate = TRUE) {

  #' Stop
  base::stopifnot(
    !rlang::is_missing(pool), !rlang::is_missing(df), !rlang::is_missing(table)
  )

  #' Duplicate
  #' Chain
  .duplicate <- base::tolower(x = .duplicate)
  if (!.duplicate %in% c("replace", "ignore")) {
    stop("Duplicate Handling can't not be `", .duplicate, "` value. Set `.duplicate` in one of following method [replace, ignore].")
  }

  #' Exit
  if (clean_afterward) {
    on.exit(unlink(capsule))
  }

  #' Database Directory Setting
  if (!local) {
    if (any(is.null(destination), destination == "", is.na(destination))) {
      stop("Can't find Database Directory. Please using `pool_set` to set directory of database")
    }
  }

  #' Valid Data Types
  if (!inherits(df, "data.frame")) {
    df <- as.data.frame(df, row.names = FALSE)
    if (!inherits(df, "data.frame")) {
      stop("Can't parse df to data.frame")
    }
  }

  #' Header
  db_header <- DBI::dbListFields(pool, name = DBI::dbQuoteIdentifier(pool, table))

  #' Validate Number of Column
  #' Columns Exists
  if (!is.null(column)) {
    if (!all(column %in% db_header)) {
      stop(
        "Dataframe `df` doesn't match header in `", table,
        "`, included ", paste0(column[which(!column %in% db_header)], collapse = ", ")
      )
    }

    #' Just In Column
    column_position <- db_header[db_header %in% column]

    #' Select Column
    df <- df %>%
      tidytable::select.(tidyselect::all_of(column_position)) %>%
      tidytable::relocate.(tidyselect::all_of(column_position))
  } else {

    #' Name included
    if (!all(names(df) %in% db_header)) {
      stop("df doesn't match header in header")
    }

    #' Relocate
    df <- df %>%
      tidytable::select.(tidyselect::all_of(db_header)) %>%
      tidytable::relocate.(tidyselect::all_of(db_header))
  }

  #' Assign
  file_name <- "component"
  file_ext <- "txt"
  .valid <- FALSE

  #' Extension Valid
  if (file_ext %not_in% c("csv", "txt")) {
    stop("Extenstion `", file_ext, "` not support")
  }

  #' File
  .file <- paste0(file_name, ".", file_ext)

  #' Transfer Path
  transfer_path <- file.path(capsule, .file)

  #' Write
  data.table::fwrite(
    x = df,
    file = transfer_path,
    append = FALSE, quote = TRUE,
    sep = ",", sep2 = c("", "|", ""), showProgress = FALSE,
    eol = .terminated, na = "", dec = ".", row.names = FALSE, col.names = TRUE
  )

  #' Transfer
  if (local) {

    #' Valid
    .valid <- fs::file_exists(path = transfer_path)

    #' Warn
    if (!clean_afterward & .valid) {
      cli::cli_alert_warning("File created in temporary folder with following path `{transfer_path}` but will remain in server.")
    }
  } else {

    #' Credentials
    .conf <- config::get(value = user, file = .user_config, use_parent = TRUE)

    #' Valid Remote User
    if (any(
      any(is.null(.conf$server), .conf$server == "", is.na(.conf$server)),
      any(is.null(.conf$password), .conf$password == "", is.na(.conf$password)),
      any(is.null(.conf$port), .conf$port == "", is.na(.conf$port))
    )) {
      stop("Can't find information of remote server in `", .user_config, "`")
    }

    #' Destination
    destination_folder_path <- file.path("/home", .conf$server, "data")
    destination_path <- file.path(destination_folder_path, .file)

    #' Transfer
    remote_transfer(
      path = transfer_path, new_path = destination_folder_path,
      server = .conf$server_host, port = .conf$port, user = .conf$server, password = .conf$password,
      .message = TRUE, method = "scp"
    )

    #' Valid
    .valid <- remote_file_exists(
      path = destination_path, user = .conf$server, host = .conf$server_host,
      port = .conf$port, password = .conf$password
    )
  }

  #' Status
  if (.valid) {

    #' Selector
    if (header) {
      line_selector <- "IGNORE 1 LINES"
    } else {
      line_selector <- ""
    }

    #' Path
    if (local) {
      path_selector <- transfer_path
    } else {
      path_selector <- destination_path
    }

    #' Column
    if (!is.null(column)) {
      col_selector <- paste0("(", paste0("`", column_position, "`", collapse = ", "), ")")
    } else {
      col_selector <- ""
    }

    #' Counter
    start <- Sys.time()

    #' Construct
    pool_execute(
      pool,
      modify_sql(
        pool,
        "LOAD DATA {lo} INFILE {pa} {du} INTO TABLE {tabu} 
        FIELDS TERMINATED BY ',' ENCLOSED BY '\"'
        LINES TERMINATED BY '\\n'
        {he}
        {co};",
        lo = DBI::SQL(ifelse(load_local, "LOCAL", "")),
        co = DBI::SQL(col_selector),
        pa = path_selector,
        du = DBI::SQL(base::toupper(.duplicate)),
        tabu = DBI::dbQuoteIdentifier(pool, table),
        he = DBI::SQL(line_selector)
      )
    )

    #' Mes
    cli::cli_alert_success(glue::glue("Success load data.frame with {format_number(nrow(df)*ncol(df))} observations \\
                                      [R:{format_number(nrow(df))}*C:{format_number(ncol(df))}] \\
                                      into `{cli::col_green(table)}` \\
                                      with total running time of {time_capture(Sys.time(), start)} seconds"))
  } else {

    #' Out
    cli::cli_alert_warning(glue::glue("Load fails. File not exists at `{file.path(destination)}` \\
                                      Please validate transfer file process."))
  }
}


#' Remote Transfer
#'
#' @description Transfer Using SCP
#' @details
#' Syntax of transfer:
#' <scp [OPTION] [user@]SRC_HOST:]file1 [user@]DEST_HOST:]file2>
#'
#' chown -R username directory
#'
#' Common Error
#' 1. Can't create regular file: ... Permission Deninded
#'
#' 2. Setting User with mode 666 / 777 by root user
#'
#' On Unix, if you need LOAD DATA to read from a pipe, you can use the following technique (the example loads a listing of the / directory into the table db1.t1):
#'
#' # mkfifo /mysql/data/db1/ls.dat
#' # chmod 666 /mysql/data/db1/ls.dat
#' # find / -ls > /mysql/data/db1/ls.dat &
#' # mysql -e "LOAD DATA INFILE 'ls.dat' INTO TABLE t1" db1
#'
#' It need packages sshpass in linux to work with, you can install it in cli by using: apt install sshpass
#'
#' @references
#' \url{https://unix.stackexchange.com/questions/466625/cp-cannot-create-regular-file-permission-denied}{}
#' \url{https://unix.stackexchange.com/questions/52634/error-using-scp-not-a-regular-file}{Unix Exchange}
#' @export
remote_transfer <- function(path, new_path, server = NULL, port = 22, user, password, .message = TRUE, method = "scp") {
  
  #' Stop
  base::stopifnot(
    !rlang::is_missing(path)
  )
  
  #' Status
  .is_folder <- fs::is_dir(path, follow = FALSE)
  
  #' Folder
  .folder_recursice <- ""
  
  #' Folder Status
  if (.is_folder) {
    .folder_recursice <- "-r"
  }
  
  #' Construct
  if (is.null(server)) {
    password <- ""
  } else {
    new_path <- glue::glue("{user}@{server}:{new_path}")
  }
  
  #' Transfer SCP
  base::system(
    command = glue::glue('sshpass -p "{password}" scp -P {port} {.folder_recursice} {path} {new_path}'), wait = TRUE, timeout = 600
  )
}

#' Remote File Exists
#'
#' @description validate file is exists from a remote file
#' @export
remote_file_exists <- function(path, user, host, port = 22, password = NULL) {
  
  #' Stop
  base::stopifnot(!rlang::is_missing(path))
  
  #' Echo
  status <- try(
    base::system(
      command = glue::glue('sshpass -p "{password}" ssh -t -p {port} {user}@{host} << EOF
      if [ -f "{path}" ]
      then echo TRUE;
      else echo FALSE >&2
      fi 
      exit;
      EOF'),
      intern = TRUE,
      wait = TRUE, timeout = 600
    )
  )
  
  #' If Err
  if (inherits(status, what = "try-error")) {
    status <- FALSE
  } else {
    status <- as.logical(status)
  }
  
  #' Return
  return(status)
}

#' Remote File Exists
#'
#' @description validate file is exists from a remote file
#' @export
gzip <- function(path) {
  
  #' Stop
  base::stopifnot(!rlang::is_missing(path))
  
  #' Zip Processing
  base::system2(
    command = "cd", wait = TRUE,
    args = c(path, "&&", "zip", "-r", paste0("../../", name), "./")
  )
  
  #' Transfer
  base::file.copy(from = paste0("www/temp/", name), to = file)
}

#' Pool Set
#'
#' @description set environment for using pool load wit will connect to target host
#' @details
#' Here are where the files are installed on the system:
#' All configuration files (like my.cnf) are under /etc/mysql
#' All binaries, libraries, headers, etc., are under /usr/bin and /usr/sbin
#' The data directory is under /var/lib/mysql
#'
#' 4. (Optional) Set up SSH Agent to store the keys to avoid having to re-enter passphrase at every login
#' Enter the following commands to start the agent and add the private SSH key
#' ssh-agent $BASH
#' ssh-add ~/.ssh/id_rsa
#' Type in your key’s current passphrase when asked. If you saved the private key somewhere other than the default location and name, you’ll have to specify it when adding the key.
#'
#' @references
#' \url{https://upcloud.com/community/tutorials/use-ssh-keys-authentication/}
#' \url{https://mariadb.com/kb/en/configuring-mariadb-for-remote-client-access/}{Set Data}
#' \url{https://community.spiceworks.com/topic/484926-mysql-error-ignoring-query-to-other-database}
#' @export
pool_set <- function(directory = "/var/lib/mysql") {
  
  #' Stop
  base::stopifnot(
    !rlang::is_missing(directory)
  )
  
  #' Mes
  cli::cli_alert_info("Setting global `DATABASE_DIRECTORY` path: `{directory}`")
  
  #' Set Database Directory
  Sys.setenv(DATABASE_DIRECTORY = directory)
}
