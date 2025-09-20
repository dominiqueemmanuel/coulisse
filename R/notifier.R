#' Construire un notifier
#'
#' @param mode "none" | "shiny" | "message" | function(type, text)
#' @param verbosity "silent" | "info" | "debug" | "hard_debug"
#' @return une fonction notifier(type, text, level = "info", duration = 1.1)
#' @export
bg_make_notifier <- function(mode = c("none", "shiny", "message"),
                             verbosity = c("silent","info","debug","hard_debug")) {
  mode <- mode[1]
  verbosity <- match.arg(verbosity)
  
  level_allowed <- switch(
    verbosity,
    silent = character(0L),
    info   = c("info","warning","error"),
    debug  = c("debug","info","warning","error"),
    hard_debug = c("hard_debug","debug","info","warning","error")
  )
  
  # signature UNIFIÉE partout: (type, text, level, duration, ..., .fields)
  if (is.function(mode)) {
    fun <- add_dots_to_function(mode)  
  } else if (identical(mode, "shiny")) {
    fun <- function(type, text, level = "info", duration = 1.1, ..., .fields = NULL) {
      # 1) JSON d’abord (toujours)
      path <- attr(sys.function(), "write_path", exact = TRUE)
      if (!is.null(path) && !isTRUE(.fields$.no_json)) {
        cat(.bg_to_json(type, c(list(level = level, msg = text), .fields %||% list())),
            "\n", file = path, append = TRUE)
      }
      
      
      if (!level %in% level_allowed) return(invisible())
      if (is.null(shiny::getDefaultReactiveDomain())) {
        message(sprintf("[%s] %s", toupper(type), text))
      } else {
        shiny::showNotification(
          text,
          type = switch(type, warning = "warning", error = "error", "message"),
          duration = duration
        )
      }
  
      invisible()
    }
  } else if (identical(mode, "message")) {
    fun <- function(type, text, level = "info", duration = 1.1, ..., .fields = NULL) {
      if (!level %in% level_allowed) return(invisible())
      msg <- sprintf("[%s] %s", toupper(type), text)
      if (identical(type, "error")) warning(msg, call. = FALSE) else message(msg)
      # path <- attr(fun, "write_path", exact = TRUE)
      path <- attr(sys.function(), "write_path", exact = TRUE)
      
      if (!is.null(path)) {
        cat(.bg_to_json(type, c(list(level = level, msg = text), .fields %||% list())),
            "\n", file = path, append = TRUE)
      }
      invisible()
    }
  } else { # "none"
    fun <- function(type, text, level = "info", duration = 1.1, ..., .fields = NULL) {
      # path <- attr(fun, "write_path", exact = TRUE)
      path <- attr(sys.function(), "write_path", exact = TRUE)
      
      if (!is.null(path)) {
        cat(.bg_to_json(type, c(list(level = level, msg = text), .fields %||% list())),
            "\n", file = path, append = TRUE)
      }
      invisible()
    }
  }
  
  structure(fun, class = "bg_notifier", verbosity = verbosity
            ,level_allowed = switch(
              verbosity,
              silent = character(0L),
              info   = c("info","warning","error"),
              debug  = c("debug","info","warning","error"),
              hard_debug = c("hard_debug","debug","info","warning","error")
            )
            )
}



add_dots_to_function <- function(func) {
  current_formals <- formals(func)
  new_formals <- c(current_formals, alist(... = ))
  formals(func) <- new_formals
  func
}


#' Milliseconds since epoch (helper)
#' @keywords internal
#' @return numeric milliseconds (double)
.bg_now_ms <- function() {
  as.numeric(Sys.time()) * 1000
}
#' Build one JSON line for tracing
#' @keywords internal
#' @param event character scalar
#' @param fields named list
#' @return JSON string (one line)
# R/notify.R
.bg_to_json <- function(event, fields = list()) {
  now <- Sys.time()
  base <- list(
    ts_ms  = as.numeric(now) * 1000,
    ts_s   = as.numeric(now),
    ts_iso = format(as.POSIXct(now, tz = "UTC"), "%Y-%m-%dT%H:%M:%OS3Z"),
    event  = event
  )
  jsonlite::toJSON(c(base, fields), auto_unbox = TRUE)
}


#' @keywords internal
.bg_notify_write <- function(notifier, event, level = "info", msg = NULL, fields = list()) {
  path <- attr(notifier, "write_path", exact = TRUE)
  if (is.null(path) || is.na(path) || !nzchar(path)) return(invisible(NULL))
  # on compose 'fields' comme coulisse le fait déjà (level/msg inclus)
  line_fields <- c(list(level = level), if (!is.null(msg)) list(msg = msg) else NULL, fields)
  cat(.bg_to_json(event, line_fields), "\n", file = path, append = TRUE)
  invisible(path)
}