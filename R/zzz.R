
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Default options ##############################################################
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

HDF5_options <- list(
  h5TryOpen.timeout = 0,
  h5TryOpen.interval = 0
)

.onLoad <- function(libname, pkgname) {
  op <- options()
  toset <- !(names(x = HDF5_options) %in% names(x = op))
  if (any(toset)) {
    options(HDF5_options[toset])
  }
  invisible(x = NULL)
}

#' @importFrom stats setNames
#' @importFrom dplyr `%>%`
#' @importFrom rlang `%||%`
#' @importFrom tools file_path_as_absolute
#' @importFrom utils globalVariables
#' @keywords internal
"_PACKAGE"

# quiets concerns of R CMD check re: the .'s that appear in pipelines
if (getRversion() >= "2.15.1") {
  globalVariables(c("."))
}