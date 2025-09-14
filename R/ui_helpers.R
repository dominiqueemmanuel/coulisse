#' Debug panel (UI) for job journal
#' @param journal_reactive A reactive() returning the jobs data.frame
#' @export
bg_debug_panel <- function(journal_reactive) {
  shiny::uiOutput(outputId = "bg_debug_panel", container = shiny::div)
}

#' Server binder for debug panel
#' @param output Shiny output list (server-side)
#' @param journal_reactive A reactive() returning the jobs data.frame
#' @export
bg_bind_debug_panel <- function(output, journal_reactive) {
  output$bg_debug_panel <- shiny::renderUI({
    df <- journal_reactive()
    if (is.null(df) || nrow(df) == 0) return(shiny::HTML("<em>Empty journal.</em>"))
    rows <- apply(df, 1, function(r)
      sprintf("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
              r[["id"]], r[["label"]], r[["pid"]], r[["status"]], r[["started_at"]], r[["finished_at"]]))
    shiny::HTML(sprintf("
      <h4>Debug: queue & jobs</h4>
      <table border='1' cellspacing='0' cellpadding='4'>
        <thead><tr><th>id</th><th>label</th><th>pid</th><th>status</th><th>start</th><th>finish</th></tr></thead>
        <tbody>%s</tbody></table>", paste(rows, collapse = "\n")))
  })
}
