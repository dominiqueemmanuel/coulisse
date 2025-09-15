# R/utils.R

`%||%` <- function(x, y) if (is.null(x)) y else x

# Build the env for callr workers.
.bg_fast_env <- function(r_default_packages = c("base","stats","utils"), extra_env = NULL) {
  val <- paste(r_default_packages, collapse = ",")
  env <- c("R_DEFAULT_PACKAGES" = val)
  if (!is.null(extra_env)) {
    stopifnot(is.character(extra_env))
    env <- c(env, extra_env)
  }
  env
}

# Start a fast background process (r_bg) with minimal profiles.
bg_fast_start <- function(fun, args = list(),
                          r_default_packages = c("base","stats","utils"),
                          extra_env = NULL) {
  stopifnot(is.function(fun))
  callr::r_bg(
    fun,
    args            = args,
    supervise       = TRUE,
    system_profile  = FALSE,
    user_profile    = FALSE,
    env             = .bg_fast_env(r_default_packages, extra_env),
    libpath         = .libPaths()
  )
}
