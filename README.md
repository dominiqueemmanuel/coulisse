# coulisse

Gestion de jobs non bloquants pour Shiny via `callr::r_bg` : queue FIFO, timeouts,
serialisation rapide `qs` (avec fallback), notifications configurables.

## Installation

```r
# apres publication sur GitHub
# remotes::install_github("tonCompte/coulisse")
```

## Exemple minimal

Voir `inst/shiny-examples/demo_app/app.R`.

## API

- `bg_manager_create()` — cree un manager (retourne $run(), $kill(), $kill_all(), $journal(), ...)
- `bg_make_notifier()` — fabrique un notifier
- `bg_debug_panel()` + `bg_bind_debug_panel()` — panneau debug optionnel
