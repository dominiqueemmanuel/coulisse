#' Créer un gestionnaire de jobs en arrière-plan
#'
#' Construit et initialise un \emph{manager} pour soumettre et suivre des jobs
#' (processus R) démarrés via \code{callr::r_bg()}, avec collecte des résultats,
#' gestion des timeouts, callbacks, et intégration Shiny.
#'
#' Deux modes de réveil (watchers) sont disponibles :
#' \itemize{
#' \item \strong{"beat"} (historique) : une boucle \code{observe()} + \code{invalidateLater()} réveille
#'   régulièrement la logique (polling périodique).
#' \item \strong{"signals"} (nouveau) : un \code{reactivePoll()} observe un répertoire de \emph{signaux}
#'   (fichiers \code{*.sig}) écrits par les processus enfants ; ne relit que si quelque chose a changé.
#' }
#'

#' @param session domaine reactif Shiny
#' @param max_workers nombre maximal de jobs en parallele
#' @param default_timeout_ms timeout d'execution par job (ms)
#' @param start_timeout_ms timeout de demarrage (ms) (NA = desactive)
#' @param beat_fast_ms periode de poll quand il y a du travail (ms)
#' @param beat_slow_ms periode de poll quand idle (ms)
#' @param default_use_qs TRUE = serialiser via qs par defaut, sinon retour direct
#' @param cleanup_qs TRUE = supprime le fichier .qs apres lecture
#' @param notify "none"|"shiny"|"message"|function(type,text)
#' @param verbosity "silent"|"info"|"debug"|"hard_debug"
#' @param queue_limit taille max de la file (Inf = illimite)
#' @param idle_shutdown_s reserve pour engine 'session' (non utilise ici)
#' @param r_default_packages vector of package names for R_DEFAULT_PACKAGES in workers
#' @param extra_env named character vector of extra env vars for workers (optional)
#' @param write_notify_table logical. Si TRUE, ecrit un log JSON Lines de tous les notify
#' @param notify_dir dossier ou ecrire le/les fichiers JSONL
#' @param fast_collect logical (si TRUE, court-cictuite le get_result et  lis directement le .qs)
#' @param watcher \code{"beat"} (par défaut) ou \code{"signals"}. Sélectionne le mécanisme de réveil.
#' @param signals_dir Chemin du répertoire où les processus enfants écrivent les fichiers
#'   de signal (\code{*.sig}) en mode \code{watcher = "signals"}. Par défaut \code{tempdir()}.
#' @param signals_interval_ms Intervalle (ms) du \code{reactivePoll()} pour contrôler la
#'   fréquence de vérification de la \code{mtime} des fichiers de signaux.
#' @param timeout_fallback_ms Intervalle (ms) d'un tick de secours \emph{léger} utilisé
#'   uniquement lorsqu'il y a des jobs actifs, afin d'évaluer les timeouts de démarrage/exécution
#'   même si aucun nouveau signal n'arrive. Ignoré lorsqu'il n'y a pas de jobs actifs.
#'
#' @return Un objet \code{coulisse_manager}.
#' @examples
#' \dontrun{
#' mgr <- bg_manager_create(watcher = "signals",
#'                          signals_dir = tempdir(),
#'                          signals_interval_ms = 500L)
#' }
#' @export
bg_manager_create <- function(
    session = shiny::getDefaultReactiveDomain(),
    max_workers = 1L,
    default_timeout_ms = 120000L,
    start_timeout_ms = NA_integer_,
    beat_fast_ms = 200L,
    beat_slow_ms = 3000L,
    default_use_qs = TRUE,
    cleanup_qs = TRUE,
    notify = "none",
    verbosity = "info",
    queue_limit = Inf,
    idle_shutdown_s = 300L,
    r_default_packages = c("base","stats","utils"),
    extra_env = NULL,
    write_notify_table = FALSE,
    notify_dir = tempdir(),
    fast_collect = TRUE,
    watcher = c("signals","beat"),
    signals_dir = tempdir(),
    signals_interval_ms = 100L,
    timeout_fallback_ms = 3000L
) {
  stopifnot(max_workers >= 1L)
  watcher <- match.arg(watcher)
  signals_dir <- normalizePath(signals_dir, winslash = "/", mustWork = FALSE)
  if (identical(watcher, "signals") && !dir.exists(signals_dir)) dir.create(signals_dir, recursive = TRUE, showWarnings = FALSE)
  
  ## -- notifier --------------------------------------------------------------
  make_wrapped_notifier <- function(user_fun, verbosity) {
    fargs <- try(names(formals(user_fun)), silent = TRUE)
    if (inherits(fargs, "try-error") || is.null(fargs)) fargs <- character(0)
    
    f <- function(type, text, level = "info", duration = 1.1, ..., .fields = NULL) {
      call_args <- list()
      if ("type" %in% fargs)  call_args$type  <- type
      if ("text" %in% fargs)  call_args$text  <- text
      if ("level" %in% fargs) call_args$level <- level
      if ("duration" %in% fargs) call_args$duration <- duration
      if ("..." %in% fargs) {
        dots <- list(...)
        if (length(dots)) call_args <- c(call_args, dots)
      }
      try(do.call(user_fun, call_args), silent = TRUE)
      path <- attr(sys.function(), "write_path", exact = TRUE)
      if (!is.null(path)) {
        cat(.bg_to_json(type, c(list(level = level, msg = text), .fields %||% list())),
            "\n", file = path, append = TRUE)
      }
      invisible()
    }
    structure(f, class = "bg_notifier", verbosity = verbosity)
  }
  
  notifier <- if (is.character(notify)) {
    bg_make_notifier(notify, verbosity)
  } else if (is.function(notify)) {
    make_wrapped_notifier(notify, verbosity)
  } else {
    bg_make_notifier("none", "silent")
  }
  
  .notify <- function(type, text, level = "info", ..., .fields = NULL) {
    notifier(type, text, level = level, ..., .fields = .fields)
  }
  
  ## écriture JSONL si demandé
  if (isTRUE(write_notify_table)) {
    dir.create(notify_dir, showWarnings = FALSE, recursive = TRUE)
    path <- file.path(notify_dir, sprintf("notify-%s.jsonl", format(Sys.time(), "%Y%m%d-%H%M%S")))
    attr(notifier, "write_path") <- path
    .notify("debug", sprintf("notify file: %s", normalizePath(path)),
            level = "debug", .fields = list(event = "notify_file"))
  }
  
  # --- exposer un émetteur public pour l'appli hôte -------------------------
  notify_emit <- function(event,
                          fields = list(),
                          level  = "hard_debug",
                          msg    = NULL,
                          ui     = FALSE,
                          type   = "info") {
    .bg_notify_write(notifier, event = event, level = level, msg = msg, fields = fields)
    if (isTRUE(ui) && !is.null(msg)) {
      safe_fields <- c(list(event = event, .no_json = TRUE, origin = "ui_notify"), fields)
      try(.notify(type, msg, level = level, .fields = safe_fields))
    }
    invisible(TRUE)
  }
  
  ## -- journal réactif -------------------------------------------------------
  journal <- shiny::reactiveVal(
    data.frame(
      id = character(), label = character(), pid = integer(),
      status = factor(character(), levels = c("queued","running","done","error","killed","timeout")),
      started_at = as.POSIXct(character()), finished_at = as.POSIXct(character()),
      stringsAsFactors = FALSE
    )
  )
  
  ## -- état interne ----------------------------------------------------------
  st <- new.env(parent = emptyenv())
  st$queue  <- list()
  st$active <- list()
  st$maxw   <- as.integer(max_workers)
  st$r_default_packages <- r_default_packages
  st$extra_env          <- extra_env
  st$ui_seen_queued <- character(0)
  
  
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
  
  ## -- API: run --------------------------------------------------------------
  run <- function(fun_or_expr, args = list(), label = NULL,
                  on_success = NULL, on_error = NULL, on_timeout = NULL, on_final = NULL,
                  trace = NULL, timeout_ms = NULL, use_qs = NULL) {
    on.exit({
      rv_beat(shiny::isolate(rv_beat())+1L)
    })
    
    if (length(st$queue) >= queue_limit) {
      .notify("warning", sprintf("Queue full (%s). Job refused.", queue_limit), level = "info")
      return(invisible(NULL))
    }
    
    callable <- wrap_callable(fun_or_expr)
    
    id <- new_id()
    
    meta <- list(id = id, label = if (is.null(label)) paste0("job@", id) else label)
    
    ## tracing enqueue_start
    t_enq0 <- .bg_now_ms()
    
    .notify("hard_debug", "[enq] start",
            level = "hard_debug",
            .fields = list(job_id = id, phase = "enqueue_start"))
    
    st$queue <- c(st$queue, list(list(
      id = id, callable = callable, args = args, meta = meta,
      on_success = on_success, on_error = on_error, on_timeout = on_timeout, on_final = on_final,
      trace = trace,
      timeout_ms = as.integer(timeout_ms %||% default_timeout_ms),
      start_timeout_ms = if (is.null(start_timeout_ms)) NA_integer_ else as.integer(start_timeout_ms),
      use_qs = isTRUE(use_qs %||% default_use_qs),
      qs_path = NULL, enqueued_at = Sys.time(), started_at = NA,
      spawn_t0_ms = NA_integer_, child_log = NA_character_
    )))

add_row(data.frame(
  id = id, label = meta$label, pid = NA_integer_,
  status = factor("queued", levels = c("queued","running","done","error","killed","timeout")),
  started_at = as.POSIXct(NA), finished_at = as.POSIXct(NA), stringsAsFactors = FALSE
))

## tracing enqueue_end
.notify("hard_debug", "[queue] enqueued",
        level = "hard_debug",
        .fields = list(job_id = id, phase = "enqueue_end",
                       dt_ms = .bg_now_ms() - t_enq0))


.notify("info", sprintf("[queued] %s added to queue", meta$label), level = "info")

invisible(id)
  }
  
  ## -- API: kill -------------------------------------------------------------
  kill <- function(id) {
    on.exit({
      rv_beat(shiny::isolate(rv_beat())+1L)
    })
    if (!is.null(st$active[[id]])) {
      x <- st$active[[id]]; p <- x$proc
      if (!is.null(p) && p$is_alive()) p$kill()
      set_status(id, "killed", TRUE)
      if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
      if (is.function(x$on_final)) try(x$on_final("killed", x$meta), silent = TRUE)
      st$active[[id]] <- NULL
      .notify("warning", sprintf("[killed] %s (active)", x$meta$label), level = "info",
              .fields = list(job_id = id, phase = "killed_active"))
      return(invisible(TRUE))
    }
    qi <- vapply(st$queue, function(e) identical(e$id, id), logical(1))
    if (any(qi)) {
      x <- st$queue[[which(qi)[1]]]; st$queue <- st$queue[!qi]
      set_status(id, "killed", TRUE)
      if (is.function(x$on_final)) try(x$on_final("killed", x$meta), silent = TRUE)
      .notify("warning", sprintf("[killed] %s removed from queue", x$meta$label), level = "info",
              .fields = list(job_id = id, phase = "killed_queued"))
      return(invisible(TRUE))
    }
    
    invisible(FALSE)
  }
  
  kill_all <- function() {
    ids_active <- names(st$active); lapply(ids_active, kill)
    ids_queue  <- vapply(st$queue, `[[`, character(1), "id"); lapply(ids_queue, kill)
    invisible(NULL)
  }
  
  ## -- démarrage des jobs ----------------------------------------------------
  start_next_if_possible <- function() {
    while (length(st$active) < st$maxw && length(st$queue) > 0L) {
      x <- st$queue[[1]]; st$queue <- st$queue[-1]
      qs_path <- if (isTRUE(x$use_qs)) {
        normalizePath(file.path(tempdir(), paste0("bg_", x$id, ".qs")), winslash = "/", mustWork = FALSE)
      } else NULL
      
      child_write_path <- attr(notifier, "write_path", exact = TRUE)
      
      child_fun <- function(callable, args, use_qs, qs_path, job_id, child_log, signals_dir = NULL, watcher = NULL) {
        now_ms  <- function() as.numeric(Sys.time()) * 1000
        to_json <- function(event, fields = list()) {
          now <- Sys.time()
          base <- list(
            ts_ms  = as.numeric(now) * 1000,
            ts_s   = as.numeric(now),
            ts_iso = format(as.POSIXct(now, tz = "UTC"), "%Y-%m-%dT%H:%M:%OS3Z"),
            event  = event
          )
          jsonlite::toJSON(c(base, fields), auto_unbox = TRUE)
        }
        child_notify <- function(event, fields = list(), msg = NULL) {
          if (!is.null(child_log)) cat(to_json(event, fields), "\n", file = child_log, append = TRUE)
          if (!is.null(msg)) cat(sprintf("[%s] %s\n", toupper(event), msg))
        }
        
        t_boot0 <- now_ms()
        child_notify("child_boot_start", list(job_id = job_id))
        
        child_notify("child_boot_end", list(job_id = job_id, dt_ms = now_ms() - t_boot0))
        
        t_u0 <- now_ms()
        child_notify("user_fn_start", list(job_id = job_id))
        out <- tryCatch(
          do.call(callable, args),
          error = function(e) { child_notify("child_error", list(job_id = job_id, message = conditionMessage(e))); if (!is.null(signals_dir) && identical(watcher,"signals")) {
            fn <- file.path(signals_dir, paste0(job_id, ".sig")); cat(paste0("error ", as.integer(now_ms())), file = fn) }; stop(e) }
        )
        child_notify("user_fn_end", list(job_id = job_id, dt_ms = now_ms() - t_u0))
        
        if (isTRUE(use_qs)) {
          t_s0 <- now_ms()
          child_notify("serialize_result_start", list(job_id = job_id, path = qs_path))
          if (!requireNamespace("qs", quietly = TRUE)) {
            child_notify("serialize_result_end", list(job_id = job_id, dt_ms = now_ms() - t_s0, bytes = NA_integer_))
            child_notify("child_exit1", list(job_id = job_id, total_ms = now_ms() - t_boot0))
          if (!is.null(signals_dir) && identical(watcher, "signals")) { fn <- file.path(signals_dir, paste0(job_id, ".sig")); cat(paste0("done ", as.integer(now_ms())), file = fn) }
                      return(out)
          }
          qs::qsave(out, qs_path, preset = "fast")
          child_notify("serialize_result_end", list(job_id = job_id, dt_ms = now_ms() - t_s0, bytes = file.size(qs_path)))
          child_notify("child_exit2", list(job_id = job_id, total_ms = now_ms() - t_boot0))
          if (!is.null(signals_dir) && identical(watcher, "signals")) { fn <- file.path(signals_dir, paste0(job_id, ".sig")); cat(paste0("done ", as.integer(now_ms())), file = fn) }
                    return(list(.qs = qs_path))
        } else {
          child_notify("child_exit3", list(job_id = job_id, total_ms = now_ms() - t_boot0))
          if (!is.null(signals_dir) && identical(watcher, "signals")) { fn <- file.path(signals_dir, paste0(job_id, ".sig")); cat(paste0("done ", as.integer(now_ms())), file = fn) }
                    return(out)
        }
      }
      
      ## tracing spawn_start
      st$active[[x$id]] <- modifyList(x, list(qs_path = qs_path))
      st$active[[x$id]]$spawn_t0_ms <- .bg_now_ms()
      .notify("hard_debug", "[spawn] start",
              level = "hard_debug",
              .fields = list(job_id = x$id, phase = "spawn_start"))
      
      
      
      
      environment(child_fun) <- environment(x$callable)
      
    
      p <- bg_fast_start(
        child_fun,
        args = list(callable = x$callable, args = x$args,
                    use_qs = x$use_qs, qs_path = qs_path,
                    job_id = x$id, child_log = child_write_path, signals_dir = signals_dir, watcher = watcher),
        r_default_packages = st$r_default_packages,
        extra_env = st$extra_env
      )
      
      ## tracing spawn_end
      .notify("hard_debug", "[spawn] end",
              level = "hard_debug",
              .fields = list(job_id = x$id, phase = "spawn_end",
                             pid = p$get_pid(),
                             dt_ms = .bg_now_ms() - st$active[[x$id]]$spawn_t0_ms))
      
      st$active[[x$id]] <- modifyList(st$active[[x$id]],
                                      list(proc = p, started_at = Sys.time(),
                                           child_log = child_write_path))
      mark_running(x$id, p$get_pid(), st$active[[x$id]]$started_at)
      .notify("info", sprintf("[start] %s (pid %s)", x$meta$label, p$get_pid()), level = "debug",
              .fields = list(job_id = x$id, phase = "start"))
    }
  }
  
  ## -- boucle de poll --------------------------------------------------------
  t0=Sys.time()
  
  
  # Fonction pour incrémenter le déclencheur à intervalles réguliers
  rv_any_work <- shiny::reactiveVal(TRUE)
  rv_beat <- shiny::reactiveVal(0L)
  if (identical(watcher, "signals")) {
    # signals-driven ticker
    sig_reader <- shiny::reactivePoll(
      intervalMillis = as.integer(signals_interval_ms), session = session,
      checkFunc = function() {
        if (!dir.exists(signals_dir)) return("")
        p <- list.files(signals_dir, pattern="\\.sig$", full.names = TRUE, no.. = TRUE)
        if (!length(p)) return("")
        i <- file.info(p)
        paste(rownames(i), as.double(i$mtime), collapse="|")
      },
      valueFunc = function() {
        p <- list.files(signals_dir, pattern="\\.sig$", full.names = TRUE, no.. = TRUE)
        p
      }
    )
    shiny::observeEvent(sig_reader(), ignoreInit = TRUE, {
      paths <- sig_reader()
      if (length(paths)) {
        rv_beat(shiny::isolate(rv_beat())+1L)
        try(unlink(paths, force = TRUE), silent = TRUE)
      }
    })
    # fallback timer only while there are active jobs, for timeouts
    shiny::observe({
      if (!(length(st$active) > 0L)) return()
      shiny::invalidateLater(as.integer(timeout_fallback_ms), session)
      rv_beat(shiny::isolate(rv_beat())+1L)
    })
  } else {
    shiny::observe({
      shiny::invalidateLater(.interval_ms_val(),session)
      rv_beat(shiny::isolate(rv_beat())+1L)
    })
  }
  
  
  
  shiny::observeEvent(rv_beat(),{
    any_work <- (length(st$queue) + length(st$active)) > 0L
    e=rv_any_work()
    if(!identical(any_work,e)){
      rv_any_work(any_work)
      .interval_ms_val(if (any_work) as.integer(beat_fast_ms) else as.integer(beat_slow_ms))
      
      .notify("hard_debug", "poll rate" |> paste(.interval_ms_val()), .fields = list(
        phase       = "poll_tick"|> paste(.interval_ms_val()),
        lane        = "manager",
        job_id      = "change_rate_to_"|> paste(.interval_ms_val()),
        interval_ms = .interval_ms_val(),
        q_len       = length(st$queue),
        a_len       = length(st$active)
      ))
      
    }
    
    
    ### >>> AJOUT : tracer chaque tick de poll (point)
    .notify("hard_debug", "poll", .fields = list(
      phase       = "poll_tick",
      lane        = "manager",
      job_id      = "(session)",
      interval_ms = .interval_ms_val(),
      q_len       = length(st$queue),
      a_len       = length(st$active)
    ))
    ### <<< AJOUT
    
    
    ## tracer "enqueue_visible" pour les jobs que l'UI voit comme 'queued'
    df <- journal()
    queued_ids <- df$id[df$status == "queued"]
    new_visible <- setdiff(queued_ids, st$ui_seen_queued)
    
    if (length(new_visible)) {
      for (jid in new_visible) {
        .notify("hard_debug", "[ui] visible",
                level = "hard_debug",
                .fields = list(job_id = jid, phase = "enqueue_visible",
                               poll_period_ms = as.integer(.interval_ms_val()),
                               ui_ts = .bg_now_ms()))
      }
      st$ui_seen_queued <- union(st$ui_seen_queued, new_visible)
    }
    
    start_next_if_possible()
    
    if (length(st$active) == 0L) return()
    
    now <- Sys.time()
    ids <- names(st$active)
    for (id in ids) {
      x <- st$active[[id]]; p <- x$proc
      if (is.null(p)) next
      
      
      
      try(p$wait(1), silent = TRUE)  # non-blocking refresh
      
      
      
      
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
          .notify("warning", sprintf("[timeout] %s startup", x$meta$label), level = "info",
                  .fields = list(job_id = id, phase = "startup_timeout", elapsed_ms = elapsed_ms0))
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
            .notify("warning", sprintf("[timeout] %s", x$meta$label), level = "info",
                    .fields = list(job_id = id, phase = "run_timeout", elapsed_ms = elapsed_ms))
          }
        }
        next
      }
      
      ## --- FAST PATH : éviter p$get_result() si possible (use_qs & fast_collect) ----
      if (isTRUE(x$use_qs) && isTRUE(fast_collect) && !is.null(x$qs_path)) {
        t_fc0 <- .bg_now_ms()
        .notify("hard_debug", "mgr_io", .fields = list(
          phase = "read_qs_direct_begin", lane = "manager", job_id = id, path = x$qs_path
        ))
        final_val <- try(qs::qread(x$qs_path), silent = TRUE)
        .notify("hard_debug", "mgr_io", .fields = list(
          phase  = "read_qs_direct_end", lane = "manager", job_id = id,
          dt_ms  = .bg_now_ms() - t_fc0,
          bytes  = if (file.exists(x$qs_path)) file.size(x$qs_path) else NA_integer_
        ))
        
        t_cb0 <- .bg_now_ms()
        if (inherits(final_val, "try-error")) {
          set_status(id, "error", TRUE)
          if (is.function(x$on_error)) try(x$on_error(as.character(final_val), x$meta), silent = TRUE)
          if (is.function(x$on_final)) try(x$on_final("error", x$meta),    silent = TRUE)
          if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
          .notify("error", sprintf("[error] %s (qs read direct)", x$meta$label), level = "info",
                  .fields = list(job_id = id, phase = "error_qs_read_direct"))
        } else {
          set_status(id, "done", TRUE)
          if (is.function(x$on_success)) try(x$on_success(final_val, x$meta), silent = TRUE)
          if (is.function(x$on_final))   try(x$on_final("done",   x$meta),    silent = TRUE)
          if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
          .notify("info", sprintf("[done] %s", x$meta$label), level = "info",
                  .fields = list(job_id = id, phase = "done_fast_collect"))
        }
        .notify("hard_debug", "mgr_io", .fields = list(
          phase = "callbacks_end1", lane = "manager", job_id = id,
          dt_ms = .bg_now_ms() - t_cb0
        ))
        st$active[[id]] <- NULL
        next
      }
      
      ## --- SLOW PATH : on garde get_result(), mais chronométré ----------------------
      t_gr0 <- .bg_now_ms()
      .notify("hard_debug", "mgr_io", .fields = list(
        phase = "get_result_begin", lane = "manager", job_id = id
      ))
      res <- try(p$get_result(), silent = TRUE)
      .notify("hard_debug", "mgr_io", .fields = list(
        phase = "get_result_end", lane = "manager", job_id = id,
        dt_ms = .bg_now_ms() - t_gr0
      ))
      
      t_cb0 <- .bg_now_ms()
      if (inherits(res, "try-error")) {
        set_status(id, "error", TRUE)
        if (is.function(x$on_error)) try(x$on_error(as.character(res), x$meta), silent = TRUE)
        if (is.function(x$on_final)) try(x$on_final("error", x$meta),       silent = TRUE)
        if (!is.null(x$qs_path) && isTRUE(cleanup_qs) && file.exists(x$qs_path)) unlink(x$qs_path, force=TRUE)
        .notify("error", sprintf("[error] %s", x$meta$label), level = "info",
                .fields = list(job_id = id, phase = "error_child"))
      } else {
        final_val <- res
        if (is.list(res) && !is.null(res$.qs)) {
          t_rd0 <- .bg_now_ms()
          final_val <- try(qs::qread(res$.qs), silent = TRUE)
          .notify("hard_debug", "[result] deserialize_end", level = "hard_debug",
                  .fields = list(job_id = id, phase = "result_deserialize_end",
                                 dt_ms = .bg_now_ms() - t_rd0,
                                 bytes = if (file.exists(res$.qs)) file.size(res$.qs) else NA_integer_))
          if (inherits(final_val, "try-error")) {
            set_status(id, "error", TRUE)
            if (is.function(x$on_error)) try(x$on_error(as.character(final_val), x$meta), silent = TRUE)
            if (is.function(x$on_final)) try(x$on_final("error", x$meta),                silent = TRUE)
            if (file.exists(res$.qs) && isTRUE(cleanup_qs)) unlink(res$.qs, force=TRUE)
            .notify("error", sprintf("[error] %s (qs read)", x$meta$label), level = "info",
                    .fields = list(job_id = id, phase = "error_qs_read"))
            st$active[[id]] <- NULL
            next
          }
          if (file.exists(res$.qs) && isTRUE(cleanup_qs)) unlink(res$.qs, force=TRUE)
        }
        set_status(id, "done", TRUE)
        if (is.function(x$on_success)) try(x$on_success(final_val, x$meta), silent = TRUE)
        if (is.function(x$on_final))   try(x$on_final("done",   x$meta),    silent = TRUE)
        .notify("info", sprintf("[done] %s", x$meta$label), level = "info",
                .fields = list(job_id = id, phase = "done"))
      }
      .notify("hard_debug", "mgr_io", .fields = list(
        phase = "callbacks_end2", lane = "manager", job_id = id,
        dt_ms = .bg_now_ms() - t_cb0
      ))
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
    beat        = shiny::reactive(rv_beat()),
    interval_ms = interval_ms,
    notify_file = function() attr(notifier, "write_path", exact = TRUE),
    notify_dir  = function() normalizePath(notify_dir, mustWork = FALSE),
    notify_emit = notify_emit
  )
}