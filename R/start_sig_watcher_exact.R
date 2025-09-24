#' Surveillance hors réactivité des fichiers .sig (équivalent reactivePoll + observeEvent)
#'
#' @param session session Shiny.
#' @param signals_dir dossier à surveiller.
#' @param interval_ms intervalle de scan en millisecondes.
#' @param rv_beat une reactiveVal() à incrémenter de +1 à chaque déclenchement.
#' @param pattern regex des fichiers (par défaut "\\.sig$").
#' @export
start_sig_watcher_exact <- function(
    session,
    signals_dir,
    interval_ms = 1000L,
    rv_beat,
    pattern = "\\.sig$"
) {
  stopifnot(is.function(rv_beat))
  alive <- TRUE
  session$onSessionEnded(function(...) alive <<- FALSE)
  
  # -- même logique que ton checkFunc(): token = paste(chemin, mtime)
  make_token_and_paths <- function() {
    if (!dir.exists(signals_dir)) {
      return(list(token = "", paths = character()))
    }
    p <- list.files(signals_dir, pattern = pattern, full.names = TRUE, no.. = TRUE)
    if (!length(p)) {
      return(list(token = "", paths = character()))
    }
    # IMPORTANT : on ne trie pas p (list.files est déjà alpha) pour mimer file.info(p) + rownames(i)
    i <- file.info(p)
    token <- paste(rownames(i), as.double(i$mtime), collapse = "|")
    list(token = token, paths = p)
  }
  
  prev <- make_token_and_paths()$token
  
  tick <- function() {
    if (!alive) return(invisible(NULL))
    
    cur <- make_token_and_paths()
    cur_token <- cur$token
    cur_paths <- cur$paths
    
    # ==> Déclenchement si ET SEULEMENT SI le token a changé
    if (!identical(cur_token, prev)) {
      prev <<- cur_token  # maj du token observé (même si paths == 0)
      
      # ==> Comme dans ton observeEvent: agir seulement s'il y a des fichiers
      if (length(cur_paths)) {
        shiny::isolate(rv_beat(rv_beat() + 1L))
        # Et on supprime TOUS les fichiers courants (même s'ils existaient déjà)
        try(unlink(cur_paths, force = TRUE), silent = TRUE)
      }
    }
    
    later::later(tick, interval_ms / 1000)
  }
  
  tick()
}
