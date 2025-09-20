#' Lire et normaliser un log JSONL coulisse
#'
#' Lit un fichier `.jsonl` produit par `bg_manager_create(..., write_notify_table = TRUE)`
#' et ajoute les colonnes utiles à la visualisation (événement "métier", timestamps, pistes, etc.).
#'
#' Points clés :
#' - détecte les tags dans les messages `.notify()` : `[queued]`, `[start]`, `[done]`, `[timeout]`, `[error]`
#'   via `msg` ou `text`, et remappe `evt` en conséquence ;
#' - supprime d'éventuels suffixes collés au nom d'événement (ex. `"run_timeoutmanager"` → `"run_timeout"`) ;
#' - normalise quelques alias vers des statuts canoniques (`timeout`, `error`).
#'
#' @param path Chemin du fichier JSONL.
#' @param tz Fuseau horaire pour les POSIXct (défaut : `Sys.timezone()`).
#'
#' @return Un `data.frame` avec au minimum : `evt`, `start_posix`, `from_posix`, `lane`,
#'   `job_id`, `is_start`, `is_end`, `evt_base`.
#' @export
#' @importFrom jsonlite stream_in
read_coulisse_log <- function(path, tz = Sys.timezone()) {
  df <- jsonlite::stream_in(file(path), verbose = FALSE)
  ## --- NEW: drop des lignes miroir UI ----------
  if (".no_json" %in% names(df)) {
    df <- df[is.na(df$.no_json) | !df$.no_json, , drop = FALSE]
  }
  ## ---------------------------------------------
  
  # evt "métier" de base
  df$evt <- ifelse(!is.null(df$phase) & !is.na(df$phase) & nzchar(df$phase), df$phase, df$event)
  
  # -- remap des statuts via [tag] en début de msg/text
  .remap_from_text <- function(vec, evt_cur) {
    tag <- tolower(sub("^\\[([^\\]]+)\\].*$", "\\1", vec))
    has <- grepl("^\\[[^\\]]+\\]", vec %||% "")
    map <- c(queued="queued", start="start", done="done", timeout="timeout", error="error")
    tag_evt <- unname(map[tag])
    ifelse(has & !is.na(tag_evt), tag_evt, evt_cur)
  }
  if ("msg" %in% names(df))  df$evt <- .remap_from_text(df$msg,  df$evt)
  if ("text" %in% names(df)) df$evt <- .remap_from_text(df$text, df$evt)
  
  # -- retirer suffixes lane collés au nom d'evt (ex: "run_timeoutmanager")
  df$evt <- sub("(manager|worker)$", "", df$evt)
  
  # -- normaliser quelques alias vers statuts
  df$evt[df$evt %in% c("run_timeout","timeout_end","manager_timeout")] <- "timeout"
  df$evt[df$evt %in% c("run_error","child_error","manager_error")]     <- "error"
  
  
  # -- FALLBACK LARGE : si 'timeout' ou 'error' apparaît dans evt, mappe en statut
  df$evt <- tolower(df$evt)
  df$evt[grepl("timeout", df$evt, fixed = TRUE)] <- "timeout"
  df$evt[grepl("error",   df$evt, fixed = TRUE)] <- "error"
  
  
  # timestamps
  if (!"ts_ms" %in% names(df) && "ts" %in% names(df)) df$ts_ms <- as.numeric(df$ts)
  df$start_posix <- as.POSIXct(df$ts_ms/1000, origin = "1970-01-01", tz = tz)
  
  # borne gauche si dt_ms est dispo
  df$from_ms    <- ifelse(!is.na(df$dt_ms), df$ts_ms - df$dt_ms, df$ts_ms)
  df$from_posix <- as.POSIXct(df$from_ms/1000, origin = "1970-01-01", tz = tz)
  
  # lane & job (session = sans job_id)
  is_child <- grepl("^(child_|user_fn_|serialize_result_)", df$evt %||% "")
  df$lane  <- ifelse(is_child, "worker", "manager")
  df$job_id <- ifelse(is.na(df$job_id) | df$job_id == "", "(session)", df$job_id)
  df$lane[df$job_id == "(session)"] <- "session"
  
  # marquages start/end
  df$is_start <- grepl("_(start)$", df$evt %||% "")
  df$is_end   <- grepl("_(end)$",   df$evt %||% "")
  df$evt_base <- sub("_(start|end)$", "", df$evt %||% "")
  
  df
}

#' Préparer les données pour \pkg{timevis} (barres, drapeaux & runtime de job)
#'
#' - Fusionne les paires `*_start/_end` en barres avec bornes strictes ;
#' - Convertit les événements ayant `dt_ms` en barres `[ts - dt_ms ; ts]` (sans doubler des paires existantes) ;
#' - Rend les événements ponctuels en drapeaux  avec trait vertical pointillé ;
#' - Ajoute une **barre "runtime de job"** `[start → fin]` :
#'     * fin = dernière occurrence `done`/`error`/`timeout` côté manager **ou** worker ;
#'     * si `done` (et `job_runtime_range="all"`), barre sur la piste **manager** ;
#'     * si `error`/`timeout`, barre **forcée sur la piste worker** ;
#'     * si les deux lanes finissent au même instant, on **préfère worker**.
#'
#' @param df `data.frame` issu de `read_coulisse_log()`.
#' @param job_filter IDs de jobs à garder (ou `NULL`).
#' @param lanes Pistes à afficher (`c("manager","worker","session")` par défaut).
#' @param min_duration_ms Durée min (ms) pour afficher une barre ; en-dessous ⇒ point.
#' @param palette Liste pour surcharger les couleurs par catégorie (voir `.palette_cat()`).
#' @param drop_session Masquer la piste `"(session)"`.
#' @param merge_pairs Fusionner les paires `*_start/_end`.
#' @param job_runtime_range `"fail_only"` (défaut), `"all"`, `"none"`.
#'
#' @return `list(items = <data.frame>, groups = <data.frame>)` utilisable par `timevis::timevis()`.
#' @export
#' @importFrom utils modifyList
#' @importFrom stats aggregate
tv_prepare <- function(df,
                       job_filter = NULL,
                       lanes = c("manager","worker","session"),
                       min_duration_ms = 5,
                       palette = NULL,
                       drop_session = FALSE,
                       merge_pairs = TRUE,
                       job_runtime_range = c("fail_only","all","none")) {
  
  job_runtime_range <- match.arg(job_runtime_range)
  
  if (!is.null(job_filter)) df <- df[df$job_id %in% job_filter, ]
  if (isTRUE(drop_session)) {
    df <- df[df$job_id != "(session)", ]
    lanes <- setdiff(lanes, "session")
  }
  df <- df[df$lane %in% lanes, ]
  
  pal <- .palette_cat()
  if (!is.null(palette)) pal <- utils::modifyList(pal, palette)
  
  # normaliser types temps
  df$start_posix <- .fmt_posix(df$start_posix)
  if ("from_posix" %in% names(df)) df$from_posix <- .fmt_posix(df$from_posix)
  
  # ---- 1) Barres depuis paires *_start/_end --------------------------------
  ranges_pairs <- NULL
  pairs <- NULL
  if (isTRUE(merge_pairs)) {
    starts <- df[df$is_start, c("job_id","lane","evt_base","start_posix","ts_ms")]
    ends   <- df[df$is_end,   c("job_id","lane","evt_base","start_posix","ts_ms")]
    if (nrow(starts) && nrow(ends)) {
      names(starts)[names(starts)=="start_posix"] <- "start_start_posix"
      names(starts)[names(starts)=="ts_ms"]       <- "start_ts_ms"
      names(ends)[names(ends)=="start_posix"]     <- "end_start_posix"
      names(ends)[names(ends)=="ts_ms"]           <- "end_ts_ms"
      
      pairs <- merge(starts, ends, by = c("job_id","lane","evt_base"), all = FALSE)
      if (nrow(pairs)) {
        pairs$item_dt_ms <- as.numeric(pairs$end_ts_ms - pairs$start_ts_ms)
        ranges_pairs <- do.call(rbind, lapply(seq_len(nrow(pairs)), function(i) {
          r <- pairs[i, ]
          if (is.na(r$start_start_posix) || is.na(r$end_start_posix)) return(NULL)
          cls <- .classify_evt(paste0(r$evt_base,"_end"))
          col <- pal[[cls$cat]] %||% pal$other
          lane_badge <- .badge(r$lane, "lane")
          dur_badge  <- .badge(sprintf("%.2fs", max(r$item_dt_ms,0)/1000), "time")
          title <- paste0("<b>", cls$label, "</b> / job=", r$job_id, " / lane=", r$lane, "<br/>",
                          "start=", .fmt_time(r$start_start_posix),
                          " - end=", .fmt_time(r$end_start_posix),
                          " / dt=", sprintf("%.2f s", max(r$item_dt_ms,0)/1000))
          if (r$item_dt_ms <= 0 || r$item_dt_ms < min_duration_ms) {
            data.frame(
              id        = paste0(r$job_id,"_",r$lane,"_",r$evt_base,"_pair_pt_",i),
              content   = paste0(cls$label, .nbsp(), lane_badge),
              start     = r$start_start_posix,
              end       = as.POSIXct(NA_real_, origin="1970-01-01", tz=attr(r$start_start_posix,"tzone")),
              type      = "point",
              group     = paste(r$job_id, r$lane, sep=" / "),
              lane      = r$lane, job_id = r$job_id, evt = r$evt_base,
              className = "tv-flag",
              style     = paste0("color:", col, ";background:transparent;border:none;"),
              title     = title, stringsAsFactors = FALSE
            )
          } else {
            data.frame(
              id        = paste0(r$job_id,"_",r$lane,"_",r$evt_base,"_pair_",i),
              content   = paste0(cls$label, .nbsp(), dur_badge, .nbsp(), lane_badge),
              start     = r$start_start_posix, end = r$end_start_posix,
              type      = "range",
              group     = paste(r$job_id, r$lane, sep=" / "),
              lane      = r$lane, job_id = r$job_id, evt = r$evt_base,
              className = "tv-range",
              style     = paste0("background-color:", col, ";border-color:", col, ";"),
              title     = title, stringsAsFactors = FALSE
            )
          }
        }))
      }
    }
  }
  
  # ---- 2) Barres depuis dt_ms (sans doublonner les paires) -----------------
  ranges_dt <- NULL
  sub <- df[!is.na(df$dt_ms), , drop = FALSE]
  if (!is.null(pairs) && nrow(pairs)) {
    paired_keys <- unique(paste(pairs$job_id, pairs$lane, pairs$evt_base, sep = "|"))
    sub$evt_base2 <- sub("_(start|end)$", "", sub$evt %||% "")
    has_pair_key  <- paste(sub$job_id, sub$lane, sub$evt_base2, sep = "|") %in% paired_keys
    is_end_evt    <- grepl("_(end)$", sub$evt %||% "")
    sub <- sub[!(has_pair_key & is_end_evt), , drop = FALSE]
  }
  if (nrow(sub)) {
    ranges_dt <- do.call(rbind, lapply(seq_len(nrow(sub)), function(i) {
      r <- sub[i, ]; if (is.na(r$from_posix) || is.na(r$start_posix)) return(NULL)
      dt <- as.numeric(r$dt_ms)
      cls <- .classify_evt(r$evt)
      col <- pal[[cls$cat]] %||% pal$other
      lane_badge <- .badge(r$lane, "lane")
      dur_badge  <- .badge(sprintf("%.2fs", max(dt,0)/1000), "time")
      bytes_badge<- if (!is.null(r$bytes) && !is.na(r$bytes)) .badge(sprintf("%d B", r$bytes), "bytes") else ""
      if (dt <= 0 || dt < min_duration_ms) {
        title <- paste0("<b>", cls$label, "</b> / job=", r$job_id, " / lane=", r$lane, "<br/>",
                        "t=", .fmt_time(r$start_posix),
                        if (!is.na(dt)) paste0(" / dt=", sprintf("%.2f s", dt/1000)) else "")
        data.frame(
          id        = paste0(r$job_id,"_",r$lane,"_",r$evt,"_dt_pt_",i),
          content   = paste0(cls$label, .nbsp(), lane_badge),
          start     = r$start_posix, end = as.POSIXct(NA_real_, origin="1970-01-01", tz=attr(r$start_posix,"tzone")),
          type      = "point",
          group     = paste(r$job_id, r$lane, sep=" / "),
          lane      = r$lane, job_id = r$job_id, evt = r$evt,
          className = "tv-flag",
          style     = paste0("color:", col, ";background:transparent;border:none;"),
          title     = title, stringsAsFactors = FALSE
        )
      } else {
        title <- paste0("<b>", cls$label, "</b> / job=", r$job_id, " / lane=", r$lane, "<br/>",
                        "start=", .fmt_time(r$from_posix),
                        " - end=", .fmt_time(r$start_posix),
                        " / dt=", sprintf("%.2f s", dt/1000),
                        if (!is.na(r$bytes)) paste0(" / bytes=", r$bytes) else "")
        data.frame(
          id        = paste0(r$job_id,"_",r$lane,"_",r$evt,"_dt_",i),
          # content   = paste0(cls$label, .nbsp(), dur_badge, bytes_badge, lane_badge),
          content = paste0(cls$label, .nbsp(), dur_badge,
                           if (!identical(bytes_badge, "")) paste0(.nbsp(), bytes_badge) else "",
                           .nbsp(), lane_badge),
          
          start     = r$from_posix, end = r$start_posix,
          type      = "range",
          group     = paste(r$job_id, r$lane, sep=" / "),
          lane      = r$lane, job_id = r$job_id, evt = r$evt,
          className = "tv-range",
          style     = paste0("background-color:", col, ";border-color:", col, ";"),
          title     = title, stringsAsFactors = FALSE
        )
      }
    }))
  }
  
  # ---- 3) Points restants ---------------------------------------------------
  df_points <- df
  if (isTRUE(merge_pairs)) df_points <- df_points[!(df_points$is_start | df_points$is_end), , drop = FALSE]
  if (!is.null(ranges_dt) && nrow(ranges_dt)) {
    has_dt <- !is.na(df$dt_ms)
    df_points <- df_points[!has_dt, , drop = FALSE]
  }
  points <- NULL
  if (nrow(df_points)) {
    points <- do.call(rbind, lapply(seq_len(nrow(df_points)), function(i) {
      r <- df_points[i, ]; if (is.na(r$start_posix)) return(NULL)
      cls <- .classify_evt(r$evt); col <- pal[[cls$cat]] %||% pal$other
      lane_badge <- .badge(r$lane, "lane")
      title <- paste0("<b>", cls$label, "</b> / job=", r$job_id, " / lane=", r$lane, "<br/>",
                      "t=", .fmt_time(r$start_posix))
      data.frame(
        id        = paste0(r$job_id,"_",r$lane,"_",r$evt,"_pt_",i),
        content   = paste0(cls$label, .nbsp(), lane_badge),
        start     = r$start_posix, end = as.POSIXct(NA_real_, origin="1970-01-01", tz=attr(r$start_posix,"tzone")),
        type      = "point",
        group     = paste(r$job_id, r$lane, sep=" / "),
        lane      = r$lane, job_id = r$job_id, evt = r$evt,
        className = "tv-flag",
        style     = paste0("color:", col, ";background:transparent;border:none;"),
        title     = title, stringsAsFactors = FALSE
      )
    }))
  }
  
  # ---- 4) Barre runtime du job (début → dernière fin) ----------------------
  job_ranges <- NULL
  if (!identical(job_runtime_range, "none")) {
    
    end_keep <- if (job_runtime_range == "fail_only") c("error","timeout") else c("error","timeout","done")
    
    end_mgr <- df[df$lane == "manager" & df$evt %in% end_keep & df$job_id != "(session)",
                  c("job_id","evt","start_posix"), drop = FALSE]
    if (nrow(end_mgr)) end_mgr$end_lane <- "manager"
    
    end_wrk <- df[df$lane == "worker" & df$evt %in% c("error","timeout","child_error","run_timeout")
                  & df$job_id != "(session)",
                  c("job_id","evt","start_posix"), drop = FALSE]
    if (nrow(end_wrk)) {
      end_wrk$evt[end_wrk$evt == "child_error"] <- "error"
      end_wrk$evt[end_wrk$evt == "run_timeout"] <- "timeout"
      end_wrk$end_lane <- "worker"
      if (job_runtime_range == "all") {
        end_wrk <- end_wrk[end_wrk$evt %in% c("error","timeout"), , drop = FALSE]
      }
    }
    
    end_all <- rbind(end_mgr, end_wrk)
    if (nrow(end_all)) {
      # préférer worker si simultané
      end_all$lane_rank <- ifelse(end_all$end_lane == "worker", 1L, 2L)
      end_all <- end_all[order(end_all$job_id, end_all$start_posix, end_all$lane_rank), ]
      end_last <- end_all[!duplicated(end_all$job_id, fromLast = TRUE), , drop = FALSE]
      
      # début: evt "start" manager si dispo, sinon 1er event du job
      start_mgr <- df[df$lane=="manager" & df$evt=="start" & df$job_id != "(session)",
                      c("job_id","start_posix"), drop = FALSE]
      names(start_mgr)[2] <- "start_ts"
      any_first <- aggregate(start_posix ~ job_id,
                             data = df[df$job_id!="(session)", c("job_id","start_posix")], FUN = min)
      names(any_first)[2] <- "first_ts"
      
      jr <- merge(end_last, start_mgr, by = "job_id", all.x = TRUE)
      jr <- merge(jr, any_first, by = "job_id", all.x = TRUE)
      jr$start_ts <- ifelse(is.na(jr$start_ts), jr$first_ts, jr$start_ts)
      
      jr <- jr[!is.na(jr$start_ts) & jr$start_ts < jr$start_posix, , drop = FALSE]
      if (nrow(jr)) {
        jr$start_ts    <- .fmt_posix(jr$start_ts)
        jr$start_posix <- .fmt_posix(jr$start_posix)
        jr$dt_ms  <- as.numeric(difftime(jr$start_posix, jr$start_ts, units="secs")) * 1000
        
        # lane final:
        # - fail (error/timeout) -> worker
        # - done -> lane réel de fin (souvent manager)
        is_fail   <- jr$evt %in% c("error","timeout")
        jr$lane   <- ifelse(is_fail, "worker", jr$end_lane)
        jr$group  <- paste(jr$job_id, jr$lane, sep = " / ")
        
        cls_lbl  <- ifelse(is_fail, "job-runtime (fail)", "job-runtime (ok)")
        col      <- ifelse(is_fail, .palette_cat()$status, "#2BB673")
        
        dur_badge <- .badge(sprintf("%.2fs", jr$dt_ms/1000), "time")
        content   <- paste0(cls_lbl, dur_badge, .badge(jr$lane, "lane"))
        title     <- paste0("<b>", cls_lbl, "</b> / job=", jr$job_id, "<br/>",
                            "start=", .fmt_time(jr$start_ts),
                            " - end=", .fmt_time(jr$start_posix),
                            " / dt=", sprintf("%.2f s", jr$dt_ms/1000))
        style     <- paste0("background-color:", col, ";border-color:", col, ";")
        
        job_ranges <- data.frame(
          id        = paste0(jr$job_id, "_", jr$lane, "_job_runtime_", seq_len(nrow(jr))),
          content   = content,
          start     = jr$start_ts, end = jr$start_posix,
          type      = "range",
          group     = jr$group, lane = jr$lane, job_id = jr$job_id,
          evt       = paste0("job_runtime_", jr$evt),
          className = "tv-range", style = style, title = title,
          stringsAsFactors = FALSE
        )
      }
    }
  }
  
  # ---- 5) Assemblage & nettoyage -------------------------------------------
  items <- do.call(rbind, Filter(function(x) !is.null(x) && nrow(x),
                                 list(ranges_pairs, ranges_dt, points, job_ranges)))
  if (is.null(items) || !nrow(items)) {
    items <- data.frame(
      id=character(), content=character(),
      start=as.POSIXct(character()), end=as.POSIXct(character()),
      type=character(), group=character(), lane=character(), job_id=character(), evt=character(),
      className=character(), style=character(), title=character(), stringsAsFactors = FALSE
    )
  }
  items <- items[!is.na(items$start), , drop = FALSE]
  bad <- which(items$type == "range" & (is.na(items$end) | items$start >= items$end))
  if (length(bad) > 0) items <- items[-bad, , drop = FALSE]
  dedupe_key <- paste(items$group, items$evt, items$type, items$start, items$end)
  items <- items[!duplicated(dedupe_key), , drop = FALSE]
  items <- items[order(items$job_id, items$lane, items$start, items$type, items$evt), , drop = FALSE]
  
  groups <- unique(data.frame(id = items$group, content = items$group, stringsAsFactors = FALSE))
  list(items = items, groups = groups)
}

#' CSS pour drapeaux et traits pointillés
#'
#' Applique le style : drapeaux (événements ponctuels) + traits pointillés verticaux
#' et traits pointillés aux bornes des barres.
#'
#' @return Un tag `<style>` à inclure (Shiny/Viewer).
#' @export
#' @importFrom htmltools tags HTML
tv_css_flags <- function() {
  htmltools::tags$style(htmltools::HTML("
  .vis-item.tv-flag.vis-point { background: transparent; border: none; box-shadow: none; }
  .vis-item.tv-flag .vis-dot { display: none; }
  .vis-item.tv-flag { padding-left: 14px; position: relative; }
  .vis-item.tv-flag:before { content: ''; position: absolute; top: 14px; bottom: 0;
    left: 6px; border-left: 1px dashed rgba(0,0,0,.28); }
  .vis-item.tv-flag:after { content: '\\2691'; position: absolute; top: -2px; left: 0;
    font-size: 12px; line-height: 1; }
  .vis-item.tv-range { position: relative; }
  .vis-item.tv-range:before { content: ''; position: absolute; top: -2px; bottom: -2px;
    left: -1px; border-left: 1px dashed rgba(0,0,0,.20); }
  .vis-item.tv-range:after { content: ''; position: absolute; top: -2px; bottom: -2px;
    right: -1px; border-right: 1px dashed rgba(0,0,0,.20); }
  "))
}

#' Légende compacte de la timeline
#'
#' @return Un tag `<div>` avec les catégories/couleurs.
#' @export
#' @importFrom htmltools div span
tv_legend <- function() {
  pal <- .palette_cat()
  one <- function(lbl, col) {
    htmltools::div(style="display:inline-flex;align-items:center;margin-right:14px",
                   htmltools::span(style=sprintf("display:inline-block;width:12px;height:12px;background:%s;margin-right:6px;border-radius:2px", col)),
                   htmltools::span(lbl))
  }
  htmltools::div(
    one("enqueue/ui",     pal$enqueue),
    one("spawn",          pal$spawn),
    one("boot (child)",   pal$child_boot),
    one("compute",        pal$compute),
    one("serialize out",  pal$ser_out),
    one("deserialize in", pal$deser_in),
    one("status",         pal$status)
  )
}

#' Assembler le widget \pkg{timevis}
#'
#' @param items Data.frame `tv$items` (depuis `tv_prepare()`).
#' @param groups Data.frame `tv$groups` (depuis `tv_prepare()`).
#' @param add_css Injecter le CSS des drapeaux/traits.
#' @param add_legend Ajouter la légende.
#' @param options Options passées à `timevis::timevis()`.
#'
#' @return Un `htmlwidgets` prêt à afficher.
#' @export
#' @importFrom timevis timevis
#' @importFrom htmltools tagList
tv_widget <- function(items, groups, add_css = TRUE, add_legend = TRUE,
                      options = list(stack = TRUE, showCurrentTime = TRUE, zoomKey = "altKey")) {
  w <- timevis::timevis(data = items, groups = groups, options = options, showZoom = TRUE)
  parts <- list()
  if (isTRUE(add_css))    parts <- c(parts, list(tv_css_flags()))
  if (isTRUE(add_legend)) parts <- c(parts, list(tv_legend()))
  htmltools::tagList(parts, w)
}

# ---------------- helpers internes ----------------

#' @keywords internal
.classify_evt <- function(evt) {
  if (is.null(evt) || is.na(evt) || !nzchar(evt)) return(list(cat="other", label=evt))
  
  # enqueue/ui
  if (grepl("^enqueue_", evt) || evt == "enqueue_visible")
    return(list(cat="enqueue", label = sub("^enqueue_", "enqueue_", evt)))
  
  # spawn
  if (grepl("^spawn_", evt))
    return(list(cat="spawn", label = "spawn"))
  
  # worker boot
  if (grepl("^child_boot_", evt))
    return(list(cat="child_boot", label = "boot(child)"))
  
  # compute (user_fn_*)
  if (grepl("^user_fn_", evt))
    return(list(cat="compute", label = "compute"))
  
  # sérialisation worker
  if (grepl("^serialize_result_", evt))
    return(list(cat="ser_out", label = "serialize-out"))
  
  # désérialisation manager (.qs -> objet)
  if (evt == "result_deserialize_end")
    return(list(cat="deser_in", label = "deserialize-in"))
  
  # I/O manager autours de get_result()
  if (grepl("^get_result_", evt))
    return(list(cat="mgr_io", label = sub("^get_result_", "get_result_", evt)))
  
  # fin des callbacks (succès/erreur/final)
  if (grepl("^callbacks_", evt))
    return(list(cat="callbacks", label = sub("^callbacks_", "callbacks_", evt)))
  
  # tick de poll
  if (evt == "poll_tick")
    return(list(cat="poll", label = "poll"))
  
  # notifs statut côté manager
  if (evt %in% c("start","done","timeout","error","error_qs_read",
                 "manager_done_emit","manager_error_emit"))
    return(list(cat="status", label = sub("^manager_", "", evt)))
  
  # divers worker
  if (grepl("^child_", evt))
    return(list(cat="worker_misc", label = sub("^child_", "child_", evt)))
  
  list(cat="other", label=evt)
}


#' @keywords internal
.palette_cat <- function() {
  list(
    enqueue     = "#4C8BF5",
    spawn       = "#7C4DFF",
    child_boot  = "#00BCD4",
    compute     = "#2BB673",
    ser_out     = "#FF9800",
    deser_in    = "#FFB300",
    mgr_io      = "#FB8C00",  # I/O manager (autour de get_result)
    callbacks   = "#1E88E5",  # post-traitements côté manager
    poll        = "#90A4AE",  # ticks de poll
    status      = "#E53935",
    worker_misc = "#9E9E9E",
    other       = "#607D8B",
    session     = "#9E9E9E"
  )
}


#' @keywords internal
.badge <- function(text, cls = "neutral") {
  color <- switch(cls,
                  "time"   = "#263238",
                  "bytes"  = "#6A1B9A",
                  "lane"   = "#37474F",
                  "id"     = "#455A64",
                  "warn"   = "#E65100",
                  "ok"     = "#1B5E20",
                  "neutral"= "#546E7A",
                  "#546E7A")
  paste0(
    "<span style='display:inline-block;padding:0 6px;margin-left:6px;",
    "font-size:11px;border-radius:10px;background:", color, ";color:#fff'>",
    text, "</span>"
  )
}

#' @keywords internal
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0) b else a

# -- helpers POSIX sûrs ------------------------------------------------------

#' @keywords internal
.fmt_posix <- function(x, tz = Sys.timezone()) {
  if (inherits(x, "POSIXt")) return(x)
  suppressWarnings(as.POSIXct(x, origin = "1970-01-01", tz = tz))
}

#' @keywords internal
.fmt_time <- function(x, tz = Sys.timezone()) {
  x <- .fmt_posix(x, tz)
  format(x, "%H:%M:%OS2")
}

#' @keywords internal
.nbsp <- function(n = 1L) paste(rep.int("&nbsp;", n), collapse = "")