# app.R — bs4Dash + coulisse, notifications navbar + popups (sans library(coulisse))

# UI -------------------------------------------------------------------------
ui <- bs4Dash::dashboardPage(
  header = bs4Dash::dashboardHeader(
    title = "Traitements asynchrones",
    rightUi = shiny::tagList(
      # Cloche + pastille + dropdown
      shiny::tags$li(class = "nav-item dropdown",
                     shiny::tags$a(
                       id = "notif_toggle", href = "#", class = "nav-link", `data-toggle` = "dropdown",
                       shiny::icon("bell"),
                       shiny::uiOutput("notif_badge", inline = TRUE)
                     ),
                     shiny::tags$div(class = "dropdown-menu dropdown-menu-right",
                                     shiny::div(class = "dropdown-header", "Derniers traitements"),
                                     shiny::uiOutput("notif_dropdown")
                     )
      )
    )
  ),
  sidebar = bs4Dash::dashboardSidebar(
    skin = "light", minified = TRUE, collapsed = TRUE,
    bs4Dash::sidebarMenu(
      bs4Dash::menuItem("Exécutions", tabName = "jobs", icon = shiny::icon("gears"))
    )
  ),
  body = bs4Dash::dashboardBody(
    # CSS badge + JS pour remonter le clic cloche à Shiny
    shiny::tags$head(
      shiny::tags$style(HTML("
        .navbar-badge { font-size: 0.75rem; position: absolute; top: 9px; right: 8px; }
        .dropdown-menu .badge { margin-left: 6px; }
        .dropdown-item { white-space: normal; }
      ")),
      shiny::tags$script(HTML("
        $(document).on('click', 'a#notif_toggle', function(){
          Shiny.setInputValue('notif_toggle', Date.now(), {priority: 'event'});
        });
      "))
    ),
    bs4Dash::tabItems(
      bs4Dash::tabItem(
        tabName = "jobs",
        shiny::fluidRow(
          bs4Dash::box(
            title = "Lancer des traitements", width = 12, collapsible = TRUE,
            shiny::fluidRow(
              shiny::column(
                4,
                shiny::h5("Simulation (succès)"),
                shiny::numericInput("nA", "Taille (n)", 5e6, min = 1e5, step = 5e5),
                shiny::numericInput("secA", "Durée (s)", 2, min = 1, step = 1),
                shiny::checkboxInput("use_qs", "Sérialiser avec {qs}", TRUE),
                shiny::actionButton("runA", "Exécuter", class = "btn btn-primary")
              ),
              shiny::column(
                4,
                shiny::h5("Traitement avec erreur"),
                shiny::numericInput("secB", "Durée (s)", 2, min = 1, step = 1),
                shiny::div(style = "height:28px"),
                shiny::actionButton("runB", "Exécuter (erreur)", class = "btn btn-outline-danger")
              ),
              shiny::column(
                4,
                shiny::h5("Traitement avec timeout"),
                shiny::numericInput("secC", "Durée (s)", 5, min = 1, step = 1),
                shiny::numericInput("toC", "Timeout (ms)", 2000, min = 200, step = 100),
                shiny::actionButton("runC", "Exécuter (timeout)", class = "btn btn-outline-warning")
              )
            )
          )
        ),
        shiny::fluidRow(
          bs4Dash::box(
            title = "État", width = 4, status = "info", solidHeader = TRUE,
            shiny::p(shiny::strong("Statut global : "), shiny::textOutput("global", inline = TRUE)),
            shiny::p("Actifs : ", shiny::textOutput("nbRunning", inline = TRUE),
                     " | En file : ", shiny::textOutput("nbQueued", inline = TRUE)),
            shiny::hr(),
            shiny::p(shiny::em("UI vivante :")),
            shiny::verbatimTextOutput("heartbeat", placeholder = TRUE)
          ),
          bs4Dash::box(
            title = "Résultats récents", width = 8, status = "primary", solidHeader = TRUE,
            shiny::h6("Simulation (succès)"),
            shiny::verbatimTextOutput("outA", placeholder = TRUE),
            shiny::hr(),
            shiny::h6("Erreur volontaire"),
            shiny::verbatimTextOutput("outB", placeholder = TRUE),
            shiny::hr(),
            shiny::h6("Timeout"),
            shiny::verbatimTextOutput("outC", placeholder = TRUE)
          )
        )
      )
    )
  ),
  controlbar = NULL,
  title = "coulisse • démo navbar + popups"
)

# SERVER ---------------------------------------------------------------------
server <- function(input, output, session) {
  
  # 1) Popups: notifier qui affiche + joli mapping (queued/start/done/error/timeout)
  popup_notifier <- function(type, text, level = "info") {
    # détection légère du statut via tag dans 'text' (ex: "[start] ...")
    tag <- if (grepl("\\[start\\]",  text)) "start"
    else if (grepl("\\[queued\\]",  text)) "queued"
    else if (grepl("\\[done\\]",    text)) "done"
    else if (grepl("\\[timeout\\]", text)) "timeout"
    else if (grepl("\\[error\\]",   text)) "error"
    else NA_character_
    
    # couleur/emoji selon statut
    if (!is.na(tag)) {
      if (tag == "queued") {
        shiny::showNotification(gsub("\\[queued\\]\\s*", "🟦 En file — ", text),
                                type = "message", duration = 1.5)
        return(invisible())
      }
      if (tag == "start") {
        shiny::showNotification(gsub("\\[start\\]\\s*", "▶️ Démarré — ", text),
                                type = "message", duration = 1.5)
        return(invisible())
      }
      if (tag == "done") {
        shiny::showNotification(gsub("\\[done\\]\\s*", "✅ Terminé — ", text),
                                type = "message", duration = 2.5)
        return(invisible())
      }
      if (tag == "timeout") {
        shiny::showNotification(gsub("\\[timeout\\]\\s*", "⏱ Timeout — ", text),
                                type = "warning", duration = 4)
        return(invisible())
      }
      if (tag == "error") {
        shiny::showNotification(gsub("\\[error\\]\\s*", "❌ Échec — ", text),
                                type = "error", duration = 5)
        return(invisible())
      }
    }
    
    # fallback sur type
    shiny::showNotification(text,
                            type = switch(type, warning = "warning", error = "error", "message"),
                            duration = if (identical(type, "error")) 5 else 2)
  }
  
  # 2) Manager coulisse avec popup_notifier + journal pour la navbar
  mgr <- coulisse::bg_manager_create(
    session            = session,
    max_workers        = 1L,
    default_timeout_ms = 120000L,
    beat_fast_ms       = 80L,
    beat_slow_ms       = 800L,
    default_use_qs     = TRUE,
    cleanup_qs         = TRUE,
    notify             = popup_notifier, # <-- popups à chaque étape
    verbosity          = "info"
  )
  
  # Résultats console centrale
  resA <- shiny::reactiveVal(NULL)
  resB <- shiny::reactiveVal(NULL)
  resC <- shiny::reactiveVal(NULL)
  
  # 3) Centre de notifications du header (badge + dropdown)
  ack_ids <- shiny::reactiveVal(character(0))
  shiny::observeEvent(input$notif_toggle, {
    df <- mgr$journal()
    finished <- df[df$status %in% c("done","error","timeout"), "id", drop = TRUE]
    ack_ids(unique(c(ack_ids(), finished)))
  }, ignoreInit = TRUE)
  
  output$notif_badge <- shiny::renderUI({
    df <- mgr$journal()
    if (nrow(df) == 0) return(NULL)
    unread <- df[df$status %in% c("done","error","timeout") & !(df$id %in% ack_ids()), , drop = FALSE]
    n <- nrow(unread)
    if (n == 0) return(NULL)
    cls <- "badge-success"
    if (any(unread$status == "error")) cls <- "badge-danger"
    else if (any(unread$status == "timeout")) cls <- "badge-warning"
    shiny::tags$span(class = paste("badge", "navbar-badge", cls), n)
  })
  
  output$notif_dropdown <- shiny::renderUI({
    df <- mgr$journal()
    done <- df[df$status %in% c("done","error","timeout"), , drop = FALSE]
    if (nrow(done) == 0) {
      return(shiny::tags$span(class = "dropdown-item text-muted", "(aucun traitement terminé)"))
    }
    done <- done[order(done$finished_at, decreasing = TRUE), ]
    done <- head(done, 5)
    items <- lapply(seq_len(nrow(done)), function(i) {
      lab <- done$label[i]
      st  <- as.character(done$status[i])
      when <- format(done$finished_at[i], "%H:%M:%S")
      badge_cls <- switch(st,
                          "done"    = "badge-success",
                          "error"   = "badge-danger",
                          "timeout" = "badge-warning",
                          "badge-secondary"
      )
      shiny::tags$a(class = "dropdown-item", href = "#",
                    shiny::span(lab),
                    shiny::span(sprintf(" — %s", when), class = "text-muted"),
                    shiny::span(class = paste("badge", badge_cls), toupper(st))
      )
    })
    shiny::tagList(items)
  })
  
  # 4) Lancements ------------------------------------------------------------
  shiny::observeEvent(input$runA, {
    n   <- shiny::isolate(as.numeric(input$nA))
    sec <- shiny::isolate(as.numeric(input$secA))
    use_qs <- shiny::isolate(isTRUE(input$use_qs))
    label <- sprintf("Simulation n=%s (%.0fs)", format(n, big.mark = " "), sec)
    
    mgr$run(
      function(n, s) { Sys.sleep(s); x <- stats::runif(n); list(mean = mean(x), n = n, finished = Sys.time()) },
      args = list(n = n, s = sec), label = label,
      on_success = function(result, meta) {
        resA(sprintf("OK • n=%s  mean=%.6f  @%s",
                     format(result$n, big.mark = " "),
                     result$mean,
                     format(result$finished, "%H:%M:%S")))
      },
      on_error = function(msg, meta) { resA(paste("Erreur:", msg)) },
      use_qs = use_qs
    )
  }, ignoreInit = TRUE)
  
  shiny::observeEvent(input$runB, {
    sec <- shiny::isolate(as.numeric(input$secB))
    mgr$run(
      function(s) { Sys.sleep(s); stop("Erreur volontaire") },
      args = list(s = sec), label = sprintf("Traitement avec erreur (%.0fs)", sec),
      on_error = function(msg, meta) { resB(paste("Erreur:", msg)) }
    )
  }, ignoreInit = TRUE)
  
  shiny::observeEvent(input$runC, {
    sec <- shiny::isolate(as.numeric(input$secC))
    to  <- shiny::isolate(as.integer(input$toC))
    mgr$run(
      function(s) { Sys.sleep(s); paste("Terminé @", as.character(Sys.time())) },
      args = list(s = sec), label = sprintf("Traitement timeout=%dms (%.0fs)", to, sec),
      timeout_ms = to,
      on_success = function(result, meta) { resC(result) },
      on_timeout = function(meta)         { resC("Timeout atteint") },
      on_error   = function(msg, meta)    { resC(paste("Erreur:", msg)) }
    )
  }, ignoreInit = TRUE)
  
  # 5) Sorties ---------------------------------------------------------------
  output$outA <- shiny::renderPrint({ x <- resA(); cat(if (is.null(x)) "(en attente / en cours)\n" else paste0(x, "\n")) })
  output$outB <- shiny::renderPrint({ x <- resB(); cat(if (is.null(x)) "(en attente / en cours)\n" else paste0(x, "\n")) })
  output$outC <- shiny::renderPrint({ x <- resC(); cat(if (is.null(x)) "(en attente / en cours)\n" else paste0(x, "\n")) })
  
  output$global <- shiny::renderText({
    df <- mgr$journal()
    if (mgr$any_running()) "au moins un job actif"
    else if (nrow(df) > 0 && any(df$status == "queued")) "jobs en file"
    else "aucun job"
  })
  output$nbRunning <- shiny::renderText({ df <- mgr$journal(); sum(df$status == "running", na.rm = TRUE) })
  output$nbQueued  <- shiny::renderText({ df <- mgr$journal(); sum(df$status == "queued",  na.rm = TRUE) })
  output$heartbeat <- shiny::renderPrint({ shiny::invalidateLater(400, session); list(now = Sys.time(), ui_random = stats::runif(1)) })
}

shiny::shinyApp(ui, server)
