#' Creer un manager de jobs pour Shiny (engine = "bg")
#'
#' @param session domaine reactif Shiny
#' @param max_workers nombre maximal de jobs en parallele
#' @param default_timeout_ms timeout d'execution par job (ms)
#' @param start_timeout_ms timeout de demarrage (ms) (NA = desactive)
#' @param beat_fast_ms periode de poll qu&& il y a du travail (ms)
#' @param beat_slow_ms periode de poll qu&& idle (ms)
#' @param default_use_qs TRUE = serialiser via qs par defaut, sinon retour direct
#' @param cleanup_qs TRUE = supprime le fichier .qs apres lecture
#' @param notify "none"|"shiny"|"message"|function(type,text)
#' @param verbosity "silent"|"info"|"debug"
#' @param queue_limit taille max de la file (Inf = illimite)
#' @param idle_shutdown_s reserve pour engine 'session' (non utilise ici)
#' @param r_default_packages vector of package names for R_DEFAULT_PACKAGES in workers
#' @param extra_env named character vector of extra env vars for workers (optional)

#'
#' @importFrom utils modifyList
#' @return une liste: run(), kill(), kill_all(), journal(), any_running(), any_work(), beat(), interval_ms()
#' @export
bg_manager_create <- function(
  session = shiny::getDefaultReactiveDomain(),
  max_workers = 1L,
  default_timeout_ms = 120000L,
  start_timeout_ms = NA_integer_,
  beat_fast_ms = 100L,
  beat_slow_ms = 800L,
  default_use_qs = TRUE,
  cleanup_qs = TRUE,
  notify = "none",
  verbosity = "info",
  queue_limit = Inf,
  idle_shutdown_s = 300L,
  r_default_packages = c("base","stats","utils"),
  extra_env = NULL
) {
  stopifnot(max_workers >= 1L)

  .notify <- if (is.character(notify)) bg_make_notifier(notify, verbosity) else {
    if (is.function(notify)) notify else bg_make_notifier("none", "silent")
  }

  journal <- shiny::reactiveVal(
    data.frame(
      id = character(), label = character(), pid = integer(),
      status = factor(character(), levels = c("queued","running","done","error","killed","timeout")),
      started_at = as.POSIXct(character()), finished_at = as.POSIXct(character()),
      stringsAsFactors = FALSE
    )
  )

  st <- new.env(parent = emptyenv())
  st$queue  <- list()
  st$active <- list()
  st$maxw   <- as.integer(max_workers)
  st$r_default_packages <- r_default_packages
  st$extra_env          <- extra_env
  
  .beat <- shiny::reactiveVal(0L)
  .interval_ms_val <- shiny::reactiveVal(as.integer(beat_fast_ms))
  interval_ms <- shiny::reactive(.interval_ms_val())

  add_row <- function(row) { df <- journal(); journal(rbind(df, row)) }
  mark_running <- function(id, pid, started_at) {
    df <- journal(); idx <- which(df$id == id); if (!length(idx)) return()
    df$pid[idx]        <- as.integer(pid)
    df$status[idx]     <- factor("running", levels = levels(df$status))
    df$started_at[idx] <- started_at
    journal(df)
  }
  set_status <- function(id, new_status, set_finished_time = TRUE) {
    df <- journal(); idx <- which(df$id == id); if (!length(idx)) return()
    df$status[idx] <- factor(new_status, levels = levels(df$status))
    if (set_finished_time) df$finished_at[idx] <- Sys.time()
    journal(df)
  }

  new_id <- function() paste0(format(Sys.time(), "%H%M%S"), "-", sample.int(899, 1) + 100L)
  wrap_callable <- function(fun_or_expr) {
    if (is.function(fun_or_expr)) return(fun_or_expr)
    expr <- substitute(fun_or_expr); force(expr)
    function(...) eval(expr, envir = list2env(list(...), parent = baseenv()))
  }

  run <- function(fun_or_expr, args = list(), label = NULL,
                  on_success = NULL, on_error = NULL, on_timeout = NULL, on_final = NULL,
                  trace = NULL, timeout_ms = NULL, use_qs = NULL) {

    if (length(st$queue) >= queue_limit) {
      .notify("warning", sprintf("Queue full (%s). Job refused.", queue_limit), level = "info")
      return(invisible(NULL))
    }

    callable <- wrap_callable(fun_or_expr)
    id <- new_id()
    meta <- list(id = id, label = if (is.null(label)) paste0("job@", id) else label)

    st$queue <- c(st$queue, list(list(
      id = id, callable = callable, args = args, meta = meta,
      on_success = on_success, on_error = on_error, on_timeout = on_timeout, on_final = on_final,
      trace = trace,
      timeout_ms = as.integer(timeout_ms %||% default_timeout_ms),
      start_timeout_ms = if (is.null(start_timeout_ms)) NA_integer_ else as.integer(start_timeout_ms),
      use_qs = isTRUE(use_qs %||% default_use_qs),
      qs_path = NULL, enqueued_at = Sys.time(), started_at = NA
    )))

    add_row(data.frame(
      id = id, label = meta$label, pid = NA_integer_,
      status = factor("queued", levels = c("queued","running","done","error","killed","timeout")),
      started_at = as.POSIXct(NA), finished_at = as.POSIXct(NA), stringsAsFactors = FALSE
    ))

    .beat(shiny::isolate(.beat()) + 1L)
    .notify("info", sprintf("[queued] %s added to queue", meta$label), level = "info")
    invisible(id)
  }

  kill <- function(id) {
    if (!is.null(st$active[[id]])) {
      x <- st$active[[id]]; p <- x$proc
      if (!is.null(p) && p$is_alive()) p$kill()
      set_status(id, "killed", TRUE)
      if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
      if (is.function(x$on_final)) try(x$on_final("killed", x$meta), silent = TRUE)
      st$active[[id]] <- NULL
      .notify("warning", sprintf("[killed] %s (active)", x$meta$label), level = "info")
      return(invisible(TRUE))
    }
    qi <- vapply(st$queue, function(e) identical(e$id, id), logical(1))
    if (any(qi)) {
      x <- st$queue[[which(qi)[1]]]; st$queue <- st$queue[!qi]
      set_status(id, "killed", TRUE)
      if (is.function(x$on_final)) try(x$on_final("killed", x$meta), silent = TRUE)
      .notify("warning", sprintf("[killed] %s removed from queue", x$meta$label), level = "info")
      return(invisible(TRUE))
    }
    invisible(FALSE)
  }

  kill_all <- function() {
    ids_active <- names(st$active); lapply(ids_active, kill)
    ids_queue  <- vapply(st$queue, `[[`, character(1), "id"); lapply(ids_queue, kill)
    invisible(NULL)
  }

  start_next_if_possible <- function() {
    while (length(st$active) < st$maxw && length(st$queue) > 0L) {
      x <- st$queue[[1]]; st$queue <- st$queue[-1]
      qs_path <- if (isTRUE(x$use_qs)) {
        normalizePath(file.path(tempdir(), paste0("bg_", x$id, ".qs")), winslash = "/", mustWork = FALSE)
      } else NULL

      child_fun <- function(callable, args, use_qs, qs_path) {
        out <- do.call(callable, args)
        if (isTRUE(use_qs)) {
          if (!requireNamespace("qs", quietly = TRUE)) {
            return(out)
          }
          qs::qsave(out, qs_path, preset = "fast")
          return(list(.qs = qs_path))
        } else {
          return(out)
        }
      }

      p <- bg_fast_start(child_fun, args = list(callable = x$callable, args = x$args,
                                                use_qs = x$use_qs, qs_path = qs_path),
                         r_default_packages = st$r_default_packages,
                         extra_env = st$extra_env
      )
      
      st$active[[x$id]] <- modifyList(x, list(proc = p, started_at = Sys.time(), qs_path = qs_path))
      mark_running(x$id, p$get_pid(), st$active[[x$id]]$started_at)
      .notify("info", sprintf("[start] %s (pid %s)", x$meta$label, p$get_pid()), level = "debug")
    }
  }

  shiny::observe({
    any_work <- (length(st$queue) + length(st$active)) > 0L
    .interval_ms_val(if (any_work) as.integer(beat_fast_ms) else as.integer(beat_slow_ms))
    shiny::invalidateLater(.interval_ms_val(), session)
    .beat(shiny::isolate(.beat()) + 1L)

    start_next_if_possible()
    if (length(st$active) == 0L) return()

    now <- Sys.time()
    ids <- names(st$active)
    for (id in ids) {
      x <- st$active[[id]]; p <- x$proc
      if (is.null(p)) next

      try(p$wait(0), silent = TRUE)  # non-blocking refresh

      if (!is.na(x$start_timeout_ms)) {
        if (is.na(x$started_at)) x$started_at <- st$active[[id]]$started_at
        elapsed_ms0 <- as.numeric(difftime(now, x$started_at, units = "secs")) * 1000
        if (elapsed_ms0 >= x$start_timeout_ms && isTRUE(p$is_alive()) &&
            is.na(tryCatch(p$get_exit_status(), error=function(e) NA_integer_))) {
          p$kill(); set_status(id, "timeout", TRUE)
          if (is.function(x$on_timeout)) try(x$on_timeout(x$meta), silent = TRUE)
          if (is.function(x$on_final))   try(x$on_final("timeout", x$meta), silent = TRUE)
          if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
          st$active[[id]] <- NULL
          .notify("warning", sprintf("[timeout] %s startup", x$meta$label), level = "info")
          next
        }
      }

      alive   <- tryCatch(p$is_alive(),        error = function(e) FALSE)
      estatus <- tryCatch(p$get_exit_status(), error = function(e) NA_integer_)
      still_running <- alive && (is.null(estatus) || is.na(estatus))

      if (still_running) {
        to_ms <- x$timeout_ms
        if (!is.na(to_ms)) {
          elapsed_ms <- as.numeric(difftime(now, x$started_at, units = "secs")) * 1000
          if (elapsed_ms >= to_ms) {
            p$kill(); set_status(id, "timeout", TRUE)
            if (is.function(x$on_timeout)) try(x$on_timeout(x$meta), silent = TRUE)
            if (is.function(x$on_final))   try(x$on_final("timeout", x$meta), silent = TRUE)
            if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
            st$active[[id]] <- NULL
            .notify("warning", sprintf("[timeout] %s", x$meta$label), level = "info")
          }
        }
        next
      }

      res <- try(p$get_result(), silent = TRUE)
      if (inherits(res, "try-error")) {
        set_status(id, "error", TRUE)
        if (is.function(x$on_error)) try(x$on_error(as.character(res), x$meta), silent = TRUE)
        if (is.function(x$on_final)) try(x$on_final("error", x$meta), silent = TRUE)
        if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
        .notify("error", sprintf("[error] %s", x$meta$label), level = "info")
      } else {
        final_val <- res
        if (is.list(res) && !is.null(res$.qs)) {
          final_val <- try(qs::qread(res$.qs), silent = TRUE)
          if (inherits(final_val, "try-error")) {
            set_status(id, "error", TRUE)
            if (is.function(x$on_error)) try(x$on_error(as.character(final_val), x$meta), silent = TRUE)
            if (is.function(x$on_final)) try(x$on_final("error", x$meta), silent = TRUE)
            if (file.exists(res$.qs) && isTRUE(cleanup_qs)) unlink(res$.qs, force=TRUE)
            .notify("error", sprintf("[error] %s (qs read)", x$meta$label), level = "info")
            st$active[[id]] <- NULL; next
          }
          if (file.exists(res$.qs) && isTRUE(cleanup_qs)) unlink(res$.qs, force=TRUE)
        }
        set_status(id, "done", TRUE)
        if (is.function(x$on_success)) try(x$on_success(final_val, x$meta), silent = TRUE)
        if (is.function(x$on_final))   try(x$on_final("done", x$meta), silent = TRUE)
        .notify("info", sprintf("[done] %s", x$meta$label), level = "info")
      }
      st$active[[id]] <- NULL
    }

    start_next_if_possible()
  })

  list(
    run         = run,
    kill        = kill,
    kill_all    = kill_all,
    journal     = shiny::reactive(journal()),
    any_running = shiny::reactive(length(st$active) > 0L),
    any_work    = shiny::reactive((length(st$queue) + length(st$active)) > 0L),
    beat        = shiny::reactive(.beat()),
    interval_ms = interval_ms
  )
}
