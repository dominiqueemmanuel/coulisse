library(shiny)
library(coulisse)

ui <- fluidPage(
  titlePanel("coulisse demo"),
  fluidRow(
    column(6,
      numericInput("sec", "Duree (s)", 2, min = 1),
      actionButton("goA", "Lancer A (succes)"),
      actionButton("goB", "Lancer B (erreur)"),
      actionButton("kill", "Tout annuler"),
      tags$hr(),
      verbatimTextOutput("out")
    ),
    column(6,
      bg_debug_panel(NULL)
    )
  ),
  tags$hr(),
  verbatimTextOutput("heartbeat")
)

server <- function(input, output, session) {
  mgr <- bg_manager_create(
    session,
    max_workers = 1L,
    notify   = "shiny",
    verbosity= "info",
    beat_fast_ms = 80L
  )

  bg_bind_debug_panel(output, mgr$journal)

  res <- reactiveVal(NULL)

  observeEvent(input$goA, {
    s <- isolate(as.numeric(input$sec))
    mgr$run(
      function(s, n) { Sys.sleep(s); list(ts = Sys.time(), sum = sum(stats::runif(n))) },
      args = list(s = s, n = 1e6),
      label = sprintf("A(%ss)", s),
      on_success = function(v, meta) res(sprintf("OK @ %s  sum=%.3f", as.character(v$ts), v$sum)),
      on_error   = function(msg, meta) res(paste("ERROR:", msg))
    )
  })
  observeEvent(input$goB, {
    s <- isolate(as.numeric(input$sec))
    mgr$run(
      function(s) { Sys.sleep(s); stop("Erreur volontaire") },
      args = list(s = s),
      label = sprintf("B(%ss)", s),
      on_error = function(msg, meta) res(paste("ERROR:", msg))
    )
  })
  observeEvent(input$kill, { mgr$kill_all() }, ignoreInit = TRUE)

  output$out <- renderPrint({ cat(if (is.null(res())) "(en cours...)\n" else paste0(res(), "\n")) })

  output$heartbeat <- renderPrint({
    invalidateLater(250, session)
    list(now = Sys.time(), random = runif(1))
  })
}
shinyApp(ui, server)
