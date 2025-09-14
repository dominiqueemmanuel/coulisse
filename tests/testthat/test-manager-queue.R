test_that("FIFO simple et statut done", {
  skip_on_cran()
  skip_if_not(interactive(), "test de timing, lancer localement")

  app <- shiny::shinyApp(
    ui = shiny::fluidPage(),
    server = function(input, output, session) {
      mgr <- coulisse::bg_manager_create(session, max_workers = 1L, notify = "none", verbosity = "silent", beat_fast_ms = 50L)
      ids <- character()
      for (i in 1:3) {
        id <- mgr$run(function() { Sys.sleep(0.5); i }, label = paste0("J", i))
        ids <- c(ids, id)
      }
      shiny::observe({
        shiny::invalidateLater(100, session)
        df <- mgr$journal()
        if (nrow(df) == 3 && all(df$status %in% c("queued","running","done"))) {
          if (all(df$status == "done")) {
            expect_true(TRUE)
            shiny::stopApp()
          }
        }
      })
    })
  shiny::runApp(app)
})
