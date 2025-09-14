#' Construire un notifier
#'
#' @param mode "none" | "shiny" | "message" | function(type, text)
#' @param verbosity "silent" | "info" | "debug"
#' @return une fonction notifier(type, text, level = "info")
#' @export
bg_make_notifier <- function(mode = c("none", "shiny", "message"), verbosity = c("silent","info","debug")) {
  mode <- mode[1]
  verbosity <- match.arg(verbosity)

  level_allowed <- switch(
    verbosity,
    silent = character(0L),
    info   = c("info", "warning", "error"),
    debug  = c("debug","info","warning","error")
  )

  if (is.function(mode)) {
    fun <- mode
  } else if (identical(mode, "shiny")) {
    fun <- function(type, text, level = "info") {
      if (!level %in% level_allowed) return(invisible())
      if (is.null(shiny::getDefaultReactiveDomain())) { message(sprintf("[%s] %s", toupper(type), text)); return(invisible()) }
      shiny::showNotification(text, type = switch(type, warning = "warning", error = "error", "message"), duration = 1.1)
    }
  } else if (identical(mode, "message")) {
    fun <- function(type, text, level = "info") {
      if (!level %in% level_allowed) return(invisible())
      msg <- sprintf("[%s] %s", toupper(type), text)
      if (identical(type, "error")) warning(msg, call. = FALSE) else message(msg)
    }
  } else { # "none"
    fun <- function(type, text, level = "info") invisible()
  }

  structure(fun, class = "bg_notifier", verbosity = verbosity)
}
