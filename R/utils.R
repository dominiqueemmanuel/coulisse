`%||%` <- function(x, y) if (is.null(x)) y else x

.bg_fast_env <- function() c("R_DEFAULT_PACKAGES" = "base,stats")

bg_fast_start <- function(fun, args = list()) {
  stopifnot(is.function(fun))
  callr::r_bg(
    fun, args = args, supervise = TRUE,
    system_profile = FALSE,
    user_profile   = FALSE,
    env            = .bg_fast_env(),
    libpath        = .libPaths()
  )
}
