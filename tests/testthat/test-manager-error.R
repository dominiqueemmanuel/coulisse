test_that("erreur est reportee", {
  skip_on_cran()
  skip_if_not(interactive())
  app <- shiny::shinyApp(
    ui = shiny::fluidPage(),
    server = function(input, output, session) {
      mgr <- coulisse::bg_manager_create(session, notify = "none", beat_fast_ms = 50L)
      mgr$run(function() { stop("boom") }, label = "ERR")
      shiny::observe({
        shiny::invalidateLater(100, session)
        df <- mgr$journal()
        if (nrow(df) == 1 && df$status[1] == "error") { expect_true(TRUE); shiny::stopApp() }
      })
    })
  shiny::runApp(app)
})
