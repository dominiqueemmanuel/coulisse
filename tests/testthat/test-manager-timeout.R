test_that("timeout execution", {
  skip_on_cran()
  skip_if_not(interactive(), "test timing")
  app <- shiny::shinyApp(
    ui = shiny::fluidPage(),
    server = function(input, output, session) {
      mgr <- coulisse::bg_manager_create(session, default_timeout_ms = 400L, beat_fast_ms = 50L, notify = "none")
      mgr$run(function() { Sys.sleep(2) ; TRUE }, label = "TO", on_timeout = function(meta) NULL)
      shiny::observe({
        shiny::invalidateLater(120, session)
        df <- mgr$journal()
        if (nrow(df) == 1 && any(df$status %in% c("running","timeout"))) {
          if (df$status[1] == "timeout") { expect_true(TRUE); shiny::stopApp() }
        }
      })
    })
  shiny::runApp(app)
})
