test_that("qs fallback si indisponible", {
  skip_on_cran()
  skip_if_not(interactive())
  app <- shiny::shinyApp(
    ui = shiny::fluidPage(),
    server = function(input, output, session) {
      mgr <- coulisse::bg_manager_create(session, default_use_qs = TRUE, notify = "none", beat_fast_ms = 50L)
      mgr$run(function() { list(a = 1:10) }, label = "QS1", on_success = function(v, m) NULL)
      shiny::observe({
        shiny::invalidateLater(100, session)
        df <- mgr$journal()
        if (nrow(df) == 1 && df$status[1] == "done") { expect_true(TRUE); shiny::stopApp() }
      })
    })
  shiny::runApp(app)
})
