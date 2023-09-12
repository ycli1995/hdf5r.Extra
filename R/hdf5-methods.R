#' @importFrom methods is slot
NULL

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# S3 methods ###################################################################
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

## h5Exists ====================================================================

#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' h5Exists(file, "/")
#' h5Exists(file, "obs")
#' h5Exists(file, "X")
#' 
#' h5fh <- h5TryOpen(file, mode = "r")
#' h5Exists(h5fh, "obs")
#' 
#' h5obj <- h5Open(h5fh, "obs")
#' h5Exists(h5obj, "groups")
#' 
#' @rdname h5Exists
#' @export
#' @method h5Exists H5Group
h5Exists.H5Group <- function(x, name, ...) {
  if (identical(x = name, y = "/")) {
    return(TRUE)
  }
  return(tryCatch(
    expr = x$exists(name = name, ...),
    error = function(e) FALSE
  ))
}

#' @rdname h5Exists
#' @export
#' @method h5Exists H5File
h5Exists.H5File <- function(x, name, ...) {
  name <- h5AbsLinkName(name = name)
  if (identical(x = name, y = "/")) {
    return(TRUE)
  }
  return(tryCatch(
    expr = x$exists(name = name, ...),
    error = function(e) FALSE
  ))
}

#' @rdname h5Exists
#' @export
#' @method h5Exists character
h5Exists.character <- function(x, name, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5Exists(x = h5fh, name = name, ...))
}

## h5Delete ====================================================================

#' @param verbose Print progress.
#' 
#' @export
#' @rdname h5Delete
#' @method h5Delete H5Group
h5Delete.H5Group <- function(x, name, verbose = TRUE, ...) {
  .h5delete(h5obj = x, name = name, verbose = verbose, ...)
  return(invisible(x = NULL))
}

#' @export
#' @rdname h5Delete
#' @method h5Delete H5File
h5Delete.H5File <- function(x, name, verbose = TRUE, ...) {
  name <- h5AbsLinkName(name = name)
  .h5delete(h5obj = x, name = name, verbose = verbose, ...)
  return(invisible(x = NULL))
}

#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' to.file <- tempfile(fileext = ".h5")
#' file.copy(file, to.file)
#' 
#' h5Delete(to.file, "obs")
#' h5Delete(to.file, "xxxx") # Delete something not existing.
#' 
#' @export
#' @rdname h5Delete
#' @method h5Delete character
h5Delete.character <- function(x, name, verbose = TRUE, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  return(.h5delete(h5obj = h5fh, name = name, verbose = verbose, ...))
}

## H5 dataset information ======================================================

#' @export
#' @rdname H5-dataset-info
#' @method h5Dims H5D
h5Dims.H5D <- function(x, ...) {
  return(x$dims)
}

#' @param name A link in \code{file}. Must represent an H5D. Used when \code{x} 
#' is an \code{H5Group}, \code{H5File} or an HDF5 file.
#' 
#' @export
#' @rdname H5-dataset-info
#' @method h5Dims H5Group
h5Dims.H5Group <- function(x, name, ...) {
  h5d <- h5Open(x = x, name = name)
  if (!identical(x = h5d, y = x)) {
    on.exit(expr = h5d$close())
  }
  return(h5d$dims)
}

#' @export
#' @rdname H5-dataset-info
#' @method h5Dims H5File
h5Dims.H5File <- function(x, name, ...) {
  name <- h5AbsLinkName(name = name)
  h5d <- h5Open(x = x, name = name)
  if (!identical(x = h5d, y = x)) {
    on.exit(expr = h5d$close())
  }
  return(h5d$dims)
}

#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' h5obj <- h5Open(file, "X")
#' 
#' h5Dims(file, "X")
#' h5Dims(h5obj)
#' 
#' @export
#' @rdname H5-dataset-info
#' @method h5Dims character
h5Dims.character <- function(x, name, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5Dims(x = h5fh, name = name, ...))
}

#' @export
#' @rdname H5-dataset-info
#' @method h5MaxDims H5D
h5MaxDims.H5D <- function(x, ...) {
  return(x$maxdims)
}

#' @export
#' @rdname H5-dataset-info
#' @method h5MaxDims H5D
h5MaxDims.H5D <- function(x, ...) {
  return(x$maxdims)
}

#' @export
#' @rdname H5-dataset-info
#' @method h5MaxDims H5Group
h5MaxDims.H5Group <- function(x, name, ...) {
  h5d <- h5Open(x = x, name = name)
  if (!identical(x = h5d, y = x)) {
    on.exit(expr = h5d$close())
  }
  return(h5d$maxdims)
}

#' @export
#' @rdname H5-dataset-info
#' @method h5MaxDims H5File
h5MaxDims.H5File <- function(x, name, ...) {
  name <- h5AbsLinkName(name = name)
  h5d <- h5Open(x = x, name = name)
  if (!identical(x = h5d, y = x)) {
    on.exit(expr = h5d$close())
  }
  return(h5d$maxdims)
}

#' @examples
#' h5MaxDims(file, "X")
#' h5MaxDims(h5obj)
#' 
#' @export
#' @rdname H5-dataset-info
#' @method h5MaxDims character
h5MaxDims.character <- function(x, name, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5MaxDims(x = h5fh, name = name, ...))
}

## H5List ======================================================================

#' @param recursive If TRUE, the contents of the whole group hierarchy will be 
#' listed.
#' @param full.names Whether or not to return the absolute object path names.
#' @param simplify Whether or not to only return the object names.
#' @param detailed Whether or not to show the detailed information.
#' 
#' @seealso \code{\link[hdf5r]{H5Group}}\code{$ls()}
#' 
#' @export
#' @rdname h5List
#' @method h5List H5Group
h5List.H5Group <- function(
    x,
    recursive = FALSE,
    full.names = FALSE,
    simplify = TRUE,
    detailed = FALSE,
    ...
) {
  return(.h5list(
    h5obj = x, 
    recursive = recursive, 
    full.names = full.names, 
    simplify = simplify,
    detailed = detailed, 
    ...
  ))
}

#' @param name A link in file. Must refer to an H5Group. Default is "/".
#' 
#' @export
#' @rdname h5List
#' @method h5List H5File
h5List.H5File <- function(
    x,
    name = "/", 
    recursive = FALSE,
    full.names = FALSE,
    simplify = TRUE,
    detailed = FALSE,
    ...
) {
  name <- h5AbsLinkName(name = name)
  h5g <- h5Open(x = x, name = name)
  if (!identical(x = h5g, y = x)) {
    on.exit(expr = h5g$close())
  }
  return(.h5list(
    h5obj = h5g, 
    recursive = recursive, 
    full.names = full.names, 
    simplify = simplify,
    detailed = detailed, 
    ...
  ))
}

#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' 
#' h5List(file)
#' h5List(file, "obs")
#' h5List(file, recursive = TRUE)
#' h5List(file, "obs", simplify = FALSE, recursive = TRUE)
#' 
#' h5g <- h5Open(file, "obs", mode = "r")
#' h5List(h5g)
#' 
#' @export
#' @rdname h5List
#' @method h5List character
h5List.character <- function(
    x,
    name = "/", 
    recursive = FALSE,
    full.names = FALSE,
    simplify = TRUE,
    detailed = FALSE,
    ...
) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5List(
    x = h5fh,
    name = name,
    recursive = recursive,
    full.names = full.names,
    simplify = simplify,
    detailed = detailed,
    ...
  ))
}

## Create HDF5 file, group or dataset ==========================================

#' @examples
#' tmp.file <- tempfile(fileext = ".h5")
#' h5CreateFile(tmp.file)
#' 
#' @export
#' @rdname h5CreateFile
#' @method h5CreateFile character
h5CreateFile.character <- function(x, ...) {
  x <- normalizePath(path = x, mustWork = FALSE)
  if (file.exists(x)) {
    warning(x, " already exists.", immediate. = TRUE)
    return(invisible(x = NULL))
  }
  h5fh <- h5TryOpen(filename = x, mode = "w", ...)
  on.exit(expr = h5fh$close())
  return(invisible(x = NULL))
}

#' @export
#' @rdname h5CreateGroup
#' @method h5CreateGroup H5Group
h5CreateGroup.H5Group <- function(x, name, show.warnings = TRUE, ...) {
  return(.h5group_create_group(
    h5group = x, 
    name = name, 
    show.warnings = show.warnings, 
    ...
  ))
}

#' @export
#' @rdname h5CreateGroup
#' @method h5CreateGroup H5File
h5CreateGroup.H5File <- function(x, name, show.warnings = TRUE, ...) {
  name <- h5AbsLinkName(name = name)
  return(.h5group_create_group(
    h5group = x, 
    name = name, 
    show.warnings = show.warnings, 
    ...
  ))
}

#' @examples
#' tmp.file <- tempfile(fileext = ".h5")
#' h5CreateFile(tmp.file)
#' 
#' h5CreateGroup(tmp.file, "g1")
#' h5CreateGroup(tmp.file, "g2/g3")
#' 
#' @export
#' @rdname h5CreateGroup
#' @method h5CreateGroup character
h5CreateGroup.character <- function(x, name, show.warnings = TRUE, ...) {
  if (!file.exists(x)) {
    if (show.warnings) {
      warning(file, " does not exist, creating it...", immediate. = TRUE)
    }
    h5CreateFile(x = x)
  }
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  h5CreateGroup(x = h5fh, name = name, ...)
  return(invisible(x = NULL))
}

#' @param dims Dimensions of the new dataset.
#' @param maxdims The maximal dimensions of the space. Default is \code{dims}.
#' @param dtype The H5 datatype to use for the creation of the object. Must be 
#' an \code{\link[hdf5r:H5T-class]{H5T}}. If set to \code{NULL}, it will be 
#' guessed through \code{\link{h5GuessDtype}}, according to \code{storage.mode}.
#' @param storage.mode Object used to guess the HDF5 datatype. Default is 
#' \code{\link{numeric}()}.
#' @param stype 'utf8' or 'ascii7'. Passed to \code{\link{h5GuessDtype}}.
#' @param chunk_size Size of the chunk. Must have the same length as the dataset 
#' dimension. If \code{NULL}, no chunking is used. If set to \code{"auto"}, the 
#' size of each chunk will be estimated according to \code{maxdims} and the byte 
#' size of \code{dtype}, using \code{\link[hdf5r]{guess_chunks}}.
#' @param gzip_level Enable zipping at the level given here. Only if chunk_dims 
#' is not \code{NULL}.
#' @param ... Arguments passed to \code{H5File$create_dataset()}. Also see 
#' \code{[hdf5r:H5File-class]{H5File}}.
#' 
#' @export
#' @rdname h5CreateDataset
#' @method h5CreateDataset H5Group
h5CreateDataset.H5Group <- function(
    x, 
    name, 
    dims,
    dtype = NULL,
    storage.mode = numeric(),
    stype = c('utf8', 'ascii7'),
    maxdims = NULL,
    chunk_size = "auto",
    gzip_level = 6,
    ...
) {
  stype <- match.arg(arg = stype)
  return(.h5group_create_dataset(
    h5group = x, 
    name = name, 
    dims = dims,
    dtype = dtype,
    storage.mode = storage.mode,
    stype = stype,
    maxdims = maxdims,
    chunk_size = chunk_size,
    gzip_level = gzip_level,
    ...
  ))
}

#' @export
#' @rdname h5CreateDataset
#' @method h5CreateDataset H5File
h5CreateDataset.H5File <- function(
    x, 
    name, 
    dims,
    dtype = NULL,
    storage.mode = numeric(),
    stype = c('utf8', 'ascii7'),
    maxdims = NULL,
    chunk_size = "auto",
    gzip_level = 6,
    ...
) {
  stype <- match.arg(arg = stype)
  name <- h5AbsLinkName(name = name)
  return(.h5group_create_dataset(
    h5group = x, 
    name = name, 
    dims = dims,
    dtype = dtype,
    storage.mode = storage.mode,
    stype = stype,
    maxdims = maxdims,
    chunk_size = chunk_size,
    gzip_level = gzip_level,
    ...
  ))
}

#' @param overwrite Whether or not to overwrite the existing HDF5 dataset.
#' 
#' @examples
#' tmp.file <- tempfile(fileext = ".h5")
#' h5CreateFile(tmp.file)
#' 
#' m <- matrix(0, 10, 5)
#' h5CreateDataset(tmp.file, "g1/m", dim(m))
#' 
#' m2 <- c("a", "b", "c")
#' h5CreateDataset(tmp.file, "g2/m2", length(m2), storage.mode = m2)
#' 
#' @export
#' @rdname h5CreateDataset
#' @method h5CreateDataset character
h5CreateDataset.character <- function(
    x, 
    name, 
    dims,
    dtype = NULL,
    storage.mode = numeric(),
    stype = c('utf8', 'ascii7'),
    maxdims = NULL,
    overwrite = FALSE,
    chunk_size = "auto",
    gzip_level = 6,
    ...
) {
  stype <- match.arg(arg = stype)
  x <- h5Overwrite(file = x, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  return(h5CreateDataset(
    x = h5fh, 
    name = name, 
    dims = dims,
    dtype = dtype,
    storage.mode = storage.mode,
    stype = stype,
    maxdims = maxdims,
    chunk_size = chunk_size,
    gzip_level = gzip_level,
    ...
  ))
}

## Open HDF5 object ============================================================

#' @export
#' @rdname h5Open
#' @method h5Open H5Group
h5Open.H5Group <- function(x, name, ...) {
  if (identical(x = name, y = "/")) {
    return(x)
  }
  return(x$open(name = name, ...))
}

#' @export
#' @rdname h5Open
#' @method h5Open H5File
h5Open.H5File <- function(x, name, ...) {
  name <- h5AbsLinkName(name = name)
  if (identical(x = name, y = "/")) {
    return(x)
  }
  return(x$open(name = name, ...))
}

#' @param mode Passed to \code{\link{h5TryOpen}}
#' 
#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' obs <- h5Open(file, "obs")
#' stopifnot(inherits(obs, "H5Group"))
#' tsne <- h5Open(file, "obsm/tsne")
#' stopifnot(inherits(tsne, "H5D"))
#' 
#' @export
#' @rdname h5Open
#' @method h5Open character
h5Open.character <- function(
    x, 
    name, 
    mode = c("a", "r", "r+", "w", "w-", "x"), 
    ...
) {
  mode <- match.arg(arg = mode)
  h5fh <- h5TryOpen(filename = x, mode = mode)
  on.exit(expr = h5fh$close())
  return(h5Open(x = h5fh, name = name, ...))
}

## HDF5 attribute ==============================================================

#' @export
#' @rdname H5-attributs
#' @method h5Attr H5D
h5Attr.H5D <- function(x, which, ...) {
  return(.h5attr(h5obj = x, which = which, ...))
}

#' @param name Name of an existing HDF5 sub-link. Default is NULL, which will 
#' use current link.
#' 
#' @export
#' @rdname H5-attributs
#' @method h5Attr H5Group
h5Attr.H5Group <- function(x, which, name = NULL, ...) {
  if (!is.null(x = name)) {
    h5obj <- h5Open(x = x, name = name)
    if (!identical(x = h5obj, y = x)) {
      on.exit(expr = h5obj$close())
    }
    return(.h5attr(h5obj = h5obj, which = which, ...))
  }
  return(.h5attr(h5obj = x, which = which, ...))
}

#' @importFrom hdf5r H5Group
#' @export
#' @rdname H5-attributs
#' @method h5Attr H5File
h5Attr.H5File <- function(x, which, name = NULL, ...) {
  name <- h5AbsLinkName(name = name)
  h5obj <- h5Open(x = x, name = name)
  if (!identical(x = h5obj, y = x)) {
    on.exit(expr = h5obj$close())
  }
  return(.h5attr(h5obj = h5obj, which = which, ...))
}

#' @return 
#' \code{H5Attr}:
#' \itemize{
#' \item If \code{which} exists in link \code{name}, will return an R object 
#' representing the attribute. If \code{which} doesn't exist or contains empty 
#' data, will return \code{NULL}.
#' \item If \code{name} doesn't exist, will raise an error from 
#' \code{H5File$attr_exists_by_name()}.
#' }
#' 
#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' 
#' # Read H5 attribute
#' x <- h5Attr(file, "encoding-version")
#' x <- h5Attr(file, "column-order", "raw/var") ## An empty attribute
#' stopifnot(is.null(x))
#' 
#' h5obj <- h5Open(file, "raw/var")
#' x <- h5Attr(h5obj, "column-order")
#' 
#' @export
#' @rdname H5-attributs
#' @method h5Attr character
h5Attr.character <- function(x, which, name = NULL, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5Attr(x = h5fh, which = which, name = name, ...))
}

#' @importFrom hdf5r h5attr_names
#' @export
#' @rdname H5-attributs
#' @method h5AttrNames H5D
h5AttrNames.H5D <- function(x, ...) {
  return(h5attr_names(x = x))
}

#' @importFrom hdf5r h5attr_names
#' @export
#' @rdname H5-attributs
#' @method h5AttrNames H5Group
h5AttrNames.H5Group <- function(x, name = NULL, ...) {
  if (!is.null(x = name)) {
    h5obj <- h5Open(x = x, name = name)
    if (!identical(x = h5obj, y = x)) {
      on.exit(expr = h5obj$close())
    }
    return(h5attr_names(x = h5obj, ...))
  }
  return(h5attr_names(x = x))
}

#' @export
#' @rdname H5-attributs
#' @method h5AttrNames H5File
h5AttrNames.H5File <- function(x, name = NULL, ...) {
  name <- h5AbsLinkName(name = name)
  h5obj <- h5Open(x = x, name = name)
  if (!identical(x = h5obj, y = x)) {
    on.exit(expr = h5obj$close())
  }
  return(h5attr_names(x = h5obj))
}

#' @return 
#' \code{h5AttrNames} will return a character vector containing all attribute 
#' names for the given link.
#' 
#' @examples
#' # Read H5 attribute names
#' h5AttrNames(file)
#' h5AttrNames(file, "X")
#' h5AttrNames(h5obj)
#' 
#' @export
#' @rdname H5-attributs
#' @method h5AttrNames character
h5AttrNames.character <- function(x, name = NULL, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5AttrNames(x = h5fh, name = name, ...))
}

#' @export
#' @rdname H5-attributs
#' @method h5Attributes H5D
h5Attributes.H5D <- function(x, ...) {
  return(.h5attributes(h5obj = x, ...))
}

#' @export
#' @rdname H5-attributs
#' @method h5Attributes H5Group
h5Attributes.H5Group <- function(x, name = NULL, ...) {
  if (!is.null(x = name)) {
    h5obj <- h5Open(x = x, name = name)
    if (!identical(x = h5obj, y = x)) {
      on.exit(expr = h5obj$close())
    }
    return(.h5attributes(h5obj = h5obj, ...))
  }
  return(.h5attributes(h5obj = x, ...))
}

#' @export
#' @rdname H5-attributs
#' @method h5Attributes H5File
h5Attributes.H5File <- function(x, name = NULL, ...) {
  name <- h5AbsLinkName(name = name)
  h5obj <- h5Open(x = x, name = name)
  if (!identical(x = h5obj, y = x)) {
    on.exit(expr = h5obj$close())
  }
  return(.h5attributes(h5obj = h5obj, ...))
}

#' @return 
#' \code{h5Attributes} will return a list containing all attributes for the 
#' given link.
#' 
#' @examples
#' # Read all H5 attributes
#' a1 <- h5Attributes(file, "raw/var")
#' a2 <- h5Attributes(h5obj)
#' stopifnot(identical(a1, a2))
#' 
#' @export
#' @rdname H5-attributs
#' @method h5Attributes character
h5Attributes.character <- function(x, name = NULL, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5Attributes(x = h5fh, name = name, ...))
}

#' @param overwrite Whether or not to overwrite the existing HDF5 attribute.
#' @param check.scalar Whether or not to use scalar space when \code{robj} is a
#' scalar. If \code{FALSE}, the attribute written will be treated as an array.
#' @param stype Passed to \code{\link{h5GuessDtype}}
#' 
#' @export
#' @rdname H5-attributs
#' @method h5WriteAttr H5D
h5WriteAttr.H5D <- function(
    x, 
    which, 
    robj, 
    overwrite = TRUE, 
    check.scalar = TRUE,
    stype = c('utf8', 'ascii7'),
    ...
) {
  return(.h5attr_write(
    h5obj = x, 
    which = which, 
    robj = robj, 
    overwrite = overwrite, 
    check.scalar = check.scalar,
    stype = stype,
    ...
  ))
}

#' @export
#' @rdname H5-attributs
#' @method h5WriteAttr H5Group
h5WriteAttr.H5Group <- function(
    x, 
    which, 
    robj, 
    name = NULL, 
    overwrite = TRUE,
    check.scalar = TRUE,
    stype = c('utf8', 'ascii7'),
    ...
) {
  if (!is.null(x = name)) {
    h5obj <- h5Open(x = x, name = name)
    if (!identical(x = h5obj, y = x)) {
      on.exit(expr = h5obj$close())
    }
    return(.h5attr_write(
      h5obj = h5obj, 
      which = which, 
      robj = robj, 
      overwrite = overwrite, 
      check.scalar = check.scalar,
      stype = stype,
      ...
    ))
  }
  return(.h5attr_write(
    h5obj = x, 
    which = which, 
    robj = robj,
    overwrite = overwrite, 
    check.scalar = check.scalar,
    stype = stype,
    ...
  ))
}

#' @importFrom hdf5r H5File
#' @export
#' @rdname H5-attributs
#' @method h5WriteAttr H5File
h5WriteAttr.H5File <- function(
    x, 
    which, 
    robj, 
    name = NULL, 
    overwrite = TRUE, 
    check.scalar = TRUE,
    stype = c('utf8', 'ascii7'),
    ...
) {
  name <- h5AbsLinkName(name = name)
  h5obj <- h5Open(x = x, name = name)
  if (!inherits(x = h5obj, what = "H5File")) {
    on.exit(expr = h5obj$close())
  }
  return(.h5attr_write(
    h5obj = h5obj, 
    which = which, 
    robj = robj, 
    overwrite = overwrite, 
    check.scalar = check.scalar,
    stype = stype,
    ...
  ))
}

#' @examples
#' # Write H5 attribute
#' tmp.file <- tempfile(fileext = ".h5")
#' file.copy(file, tmp.file)
#' 
#' new_a <- character()  # Can write an empty attribute
#' h5WriteAttr(tmp.file, "new_a", robj = new_a, name = "X")
#' new_a <- c("a", "b")
#' h5WriteAttr(tmp.file, "new_a", robj = new_a, name = "X", overwrite = TRUE)
#' h5Attr(tmp.file, "new_a", name = "X")
#' 
#' @export
#' @rdname H5-attributs
#' @method h5WriteAttr character
h5WriteAttr.character <- function(
    x, 
    which, 
    robj, 
    name = NULL, 
    overwrite = TRUE, 
    check.scalar = TRUE,
    stype = c('utf8', 'ascii7'),
    ...
) {
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  return(h5WriteAttr(
    x = h5fh,
    name = name,
    robj = robj, 
    which = which, 
    overwrite = overwrite, 
    check.scalar = check.scalar,
    stype = stype,
    ...
  ))
}

#' @export
#' @rdname H5-attributs
#' @method h5DeleteAttr H5D
h5DeleteAttr.H5D <- function(x, which, ...) {
  return(.h5attr_delete(h5obj = x, which = which, ...))
}

#' @export
#' @rdname H5-attributs
#' @method h5DeleteAttr H5Group
h5DeleteAttr.H5Group <- function(x, which, name = NULL, ...) {
  if (!is.null(x = name)) {
    h5obj <- h5Open(x = x, name = name)
    if (!identical(x = h5obj, y = x)) {
      on.exit(expr = h5obj$close())
    }
    return(.h5attr_delete(h5obj = h5obj, which = which, ...))
  }
  return(.h5attr_delete(h5obj = x, which = which, ...))
}

#' @export
#' @rdname H5-attributs
#' @method h5DeleteAttr H5File
h5DeleteAttr.H5File <- function(x, which, name = NULL, ...) {
  name <- h5AbsLinkName(name = name)
  h5obj <- h5Open(x = x, name = name)
  if (!identical(x = h5obj, y = x)) {
    on.exit(expr = h5obj$close())
  }
  return(.h5attr_delete(h5obj = h5obj, which = which, ...))
}

#' @examples
#' # Delete H5 attribute
#' h5DeleteAttr(tmp.file, "new_a", name = "X")
#' stopifnot(is.null(h5Attr(tmp.file, "new_a", name = "X")))
#' 
#' @export
#' @rdname H5-attributs
#' @method h5DeleteAttr character
h5DeleteAttr.character <- function(x, which, name = NULL, ...) {
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  return(h5DeleteAttr(x = h5fh, which = which, name = name, ...))
}

## Read and write HDF5 data ====================================================

#' @param stype 'utf8' or 'ascii7'. Passed to \code{\link{h5GuessDtype}}.
#' 
#' @export
#' @rdname h5WriteScalar
#' @method h5WriteScalar H5Group
h5WriteScalar.H5Group <- function(
    x, 
    name, 
    robj, 
    stype = c('utf8', 'ascii7'),
    ...
) {
  stype <- match.arg(arg = stype)
  return(.h5group_write_scalar(
    h5group = x, 
    name = name, 
    robj = robj, 
    stype = stype,
    ...
  ))
}

#' @export
#' @rdname h5WriteScalar
#' @method h5WriteScalar H5File
h5WriteScalar.H5File <- function(x, name, robj, ...) {
  name <- h5AbsLinkName(name = name)
  return(.h5group_write_scalar(
    h5group = x, 
    name = name, 
    robj = robj, 
    ...
  ))
}

#' @param overwrite Whether or not to overwrite the existing \code{name}.
#' 
#' @examples
#' tmp.file <- tempfile(fileext = ".h5")
#' h5CreateFile(tmp.file)
#' 
#' h5WriteScalar(tmp.file,  name = "test/scalar", TRUE)
#' x <- h5ReadDataset(tmp.file, name = "test/scalar")
#' stopifnot(x)
#' 
#' h5WriteScalar(tmp.file,  name = "test/scalar", 100.0, overwrite = TRUE)
#' x <- h5ReadDataset(tmp.file, name = "test/scalar")
#' stopifnot(identical(x, 100.0))
#' 
#' h5WriteScalar(tmp.file,  name = "test/scalar", "ABC", overwrite = TRUE)
#' x <- h5Read(tmp.file, name = "test/scalar")
#' stopifnot(identical(x, "ABC"))
#' 
#' h5WriteScalar(tmp.file,  name = "test/factor", factor("ABC"))
#' x <- h5ReadDataset(tmp.file, name = "test/factor")
#' stopifnot(identical(x, factor("ABC")))
#' 
#' @export
#' @rdname h5WriteScalar
#' @method h5WriteScalar character
h5WriteScalar.character <- function(
    x, 
    name, 
    robj, 
    overwrite = FALSE,
    ...
) {
  x <- h5Overwrite(file = x, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  return(h5WriteScalar(x = h5fh, name = name,  robj = robj, ...))
}

#' @param idx_list The indices for each dimension of \code{name} to subset given 
#' as a list. If \code{NULL}, the entire dataset will be use. Passed to 
#' \code{\link[hdf5r:H5D-class]{H5D}}\code{$write(args)}
#' @param transpose Whether or not to transpose the input matrix. Only works for 
#' a 2-dimension array-like object.
#' @param block_size Default size for number of columns to transpose in a single 
#' writing. Increasing block_size may speed up but at an additional memory cost.
#' @param verbose Print progress.
#' 
#' @importFrom rlang is_atomic
#' @importFrom easy.utils chunkPoints
#' @importMethodsFrom Matrix t
#' @export
#' @rdname h5WriteDataset
#' @method h5WriteDataset H5D
h5WriteDataset.H5D <- function(
    x,
    robj,
    idx_list = NULL,
    transpose = FALSE,
    block_size = 5000L,
    verbose = TRUE,
    ...
) {
  if (!is_atomic(x = robj)) {
    stop("'robj' must be an atomic type.")
  }
  dims <- .get_valid_dims(x = robj, transpose = transpose)
  if (transpose) {
    idx_list <- idx_list %||% lapply(X = dims, FUN = seq_len)
  }
  .check_before_h5d_write(h5obj = x, dims = dims, idx_list = idx_list)
  if (verbose && is.list(x = idx_list)) {
    message(
      "Try to write 'robj' to ", .idx_list_to_msg(idx_list = idx_list), " ",
      "of existing H5D '", x$get_obj_name(), "'"
    )
  }
  if (!transpose) {
    x$write(args = idx_list, value = as.array(x = robj), ...)
    gc(verbose = FALSE)
    return(invisible(x = NULL))
  }
  chunks <- chunkPoints(dsize = ncol(x = robj), csize = block_size)
  for (i in seq_len(length.out = ncol(x = chunks))) {
    col_inds <- chunks[1, i]:chunks[2, i]
    tmp.robj <- t(x = robj[, col_inds, drop = FALSE])
    index <- list(idx_list[[1]][col_inds], idx_list[[2]])
    x$write(args = index, value = as.array(x = tmp.robj), ...)
  }
  gc(verbose = FALSE)
  h5garbage_collect()
  return(invisible(x = NULL))
}

#' @param name Name of the HDF5 dataset to be written.
#' 
#' @export
#' @rdname h5WriteDataset
#' @method h5WriteDataset H5Group
h5WriteDataset.H5Group <- function(
    x,
    robj,
    name,
    idx_list = NULL,
    transpose = FALSE,
    block_size = 5000L,
    verbose = TRUE,
    ...
) {
  return(.h5group_write_dataset(
    h5group = x,
    robj = robj,
    name = name,
    idx_list = idx_list,
    transpose = transpose,
    block_size = block_size,
    verbose = verbose,
    ...
  ))
}

#' @export
#' @rdname h5WriteDataset
#' @method h5WriteDataset H5File
h5WriteDataset.H5File <- function(
    x,
    robj,
    name,
    idx_list = NULL,
    transpose = FALSE,
    block_size = 5000L,
    verbose = TRUE,
    ...
) {
  name <- h5AbsLinkName(name = name)
  return(.h5group_write_dataset(
    h5group = x,
    robj = robj,
    name = name,
    idx_list = idx_list,
    transpose = transpose,
    block_size = block_size,
    verbose = verbose,
    ...
  ))
}

#' @examples
#' tmp.file <- tempfile(fileext = ".h5")
#' h5CreateFile(tmp.file)
#' 
#' # Scalar (will be written into array space for HDF5) ##########
#' h5CreateDataset(
#'   tmp.file, 
#'   name = "test/bool", 
#'   dims = 1, 
#'   storage.mode = logical()
#' ) # Must create the dataset first
#' h5WriteDataset(tmp.file, FALSE, name = "test/bool")
#' x <- h5Read(tmp.file, name = "test/bool")
#' stopifnot(!x)
#' 
#' h5CreateDataset(tmp.file, name = "test/num", dims = 1)
#' h5WriteDataset(tmp.file, 100.0, name = "test/num")
#' x <- h5Read(tmp.file, name = "test/num")
#' stopifnot(identical(x, 100.0))
#' 
#' h5CreateDataset(
#'   tmp.file, 
#'   name = "test/string", 
#'   dims = 1, 
#'   storage.mode = character()
#' )
#' h5WriteDataset(tmp.file, "ABC", name = "test/string")
#' x <- h5Read(tmp.file, name = "test/string")
#' stopifnot(identical(x, "ABC"))
#' 
#' # Vector (1d array) ##########
#' x1 <- rep(FALSE, 10)
#' h5CreateDataset(
#'   tmp.file, 
#'   name = "vec/bool", 
#'   dims = 10, 
#'   storage.mode = logical()
#' )
#' h5WriteDataset(tmp.file, x1, name = "vec/bool")
#' x <- h5Read(tmp.file, name = "vec/bool")
#' stopifnot(identical(x, x1))
#' 
#' x1 <- rep(1.1, 10)
#' h5CreateDataset(
#'   tmp.file, 
#'   name = "vec/num", 
#'   dims = 10
#' )
#' h5WriteDataset(tmp.file, x1, name = "vec/num")
#' x <- h5Read(tmp.file, name = "vec/num")
#' stopifnot(identical(x, x1))
#' 
#' x1 <- rep(2.0, 5)
#' h5WriteDataset(
#'   tmp.file, 
#'   x1, 
#'   name = "vec/num", 
#'   idx_list = list(c(1, 3, 5, 7, 9)) # Set each indices to be written
#' )
#' x <- h5Read(tmp.file, name = "vec/num")
#' stopifnot(identical(x, rep(c(2.0, 1.1), 5)))
#' 
#' # matrix ##########
#' x1 <- matrix(1.0, 7, 5)
#' h5CreateDataset(
#'   tmp.file, 
#'   name = "mat/num", 
#'   dims = dim(x1)
#' )
#' h5WriteDataset(
#'   tmp.file, 
#'   x1, 
#'   name = "mat/num"
#' )
#' x <- h5Read(tmp.file, name = "mat/num")
#' stopifnot(identical(x, x1))
#' 
#' x1 <- matrix(2.0, 3, 4)
#' h5WriteDataset(
#'   tmp.file, 
#'   x1, 
#'   name = "mat/num",
#'   idx_list = list(2:4, 1:4)
#' )
#' x <- h5Read(tmp.file, name = "mat/num")
#' print(x)
#' 
#' h5WriteDataset(
#'   tmp.file, 
#'   x1, 
#'   name = "mat/num",
#'   idx_list = list(1:4, 2:4),  # idx_list must match the transposed matrix
#'   transpose = TRUE
#' )
#' x <- h5Read(tmp.file, name = "mat/num")
#' print(x)
#' 
#' @export
#' @rdname h5WriteDataset
#' @method h5WriteDataset character
h5WriteDataset.character <- function(
    x,
    robj,
    name,
    idx_list = NULL,
    transpose = FALSE,
    block_size = 5000L,
    verbose = TRUE,
    ...
) {
  h5fh <- h5TryOpen(filename = x, mode = "r+")
  on.exit(expr = h5fh$close())
  return(h5WriteDataset(
    x = h5fh,
    robj = robj,
    name = name,
    idx_list = idx_list,
    transpose = transpose, 
    block_size = block_size,
    verbose = verbose,
    ...
  ))
}

#' @param transpose Whether or not to transpose the read matrix. Only works for 
#' a 2-dimension array-like data.
#' @param idx_list The indices for each dimension of \code{name} to subset given 
#' as a list. If \code{NULL}, the entire dataset will be read. Passed to 
#' \code{\link[hdf5r:H5D-class]{H5D}}\code{$read(args)}.
#' 
#' @export
#' @rdname h5ReadDataset
#' @method h5ReadDataset H5D
h5ReadDataset.H5D <- function(x, idx_list = NULL, transpose = FALSE, ...) {
  .check_before_h5d_read(h5obj = x, idx_list = idx_list)
  if (!transpose) {
    r_obj <- x$read(args = idx_list, ...)
    return(r_obj)
  }
  dims <- x$dims
  if (length(x = dims) != 2) {
    stop("'transpose' only works for matrix-like data")
  }
  idx_list <- idx_list %||% sapply(X = dims, FUN = seq_len, simplify = FALSE)
  r_obj.dims <- rev(x = lengths(x = idx_list))
  r_obj <- matrix(data = 0, nrow = r_obj.dims[1], ncol = r_obj.dims[2])
  for (i in seq_along(along.with = idx_list[[1]])) {
    d <- x$read(args = list(idx_list[[1]][i], idx_list[[2]]), ...)
    r_obj[, i] <- d
  }
  gc(verbose = FALSE)
  return(r_obj)
}

#' @param name Name of the HDF5 link to be read. Must be an H5 dataset.
#' 
#' @export
#' @rdname h5ReadDataset
#' @method h5ReadDataset H5Group
h5ReadDataset.H5Group <- function(
    x, 
    name, 
    idx_list = NULL, 
    transpose = FALSE, 
    ...
) {
  return(.h5group_read_dataset(
    h5group = x, 
    name = name, 
    idx_list = idx_list, 
    transpose = transpose, 
    ...
  ))
}

#' @export
#' @rdname h5ReadDataset
#' @method h5ReadDataset H5File
h5ReadDataset.H5File <- function(
    x, 
    name, 
    idx_list = NULL, 
    transpose = FALSE,
    ...
) {
  name <- h5AbsLinkName(name = name)
  return(.h5group_read_dataset(
    h5group = x, 
    name = name, 
    idx_list = idx_list, 
    transpose = transpose, 
    ...
  ))
}

#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' 
#' x <- h5ReadDataset(file, name = "X")
#' x <- h5ReadDataset(file, name = "X", transpose = TRUE)
#' x <- h5ReadDataset(file, name = "X", idx_list = list(1:10, 1:20))
#' x <- h5ReadDataset(
#'   file, 
#'   name = "X", 
#'   idx_list = list(1:10, 1:20), 
#'   transpose = TRUE
#' )
#' 
#' @export
#' @rdname h5ReadDataset
#' @method h5ReadDataset character
h5ReadDataset.character <- function(
    x, 
    name, 
    transpose = FALSE, 
    idx_list = NULL, 
    ...
) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5ReadDataset(
    x = h5fh,
    name = name,
    transpose = transpose, 
    idx_list = idx_list, 
    ...
  ))
}

#' @param name Name of the HDF5 link to be read.
#' @param transpose Whether or not to transpose the read matrix. Only works for 
#' a 2-dimension array-like data.
#' @param toS4.func A function to convert the read R list into an S4 object. 
#' 
#' @export
#' @rdname h5Read
#' @method h5Read H5Group
h5Read.H5Group <- function(
    x, 
    name = NULL, 
    transpose = FALSE, 
    toS4.func = NULL, 
    ...
) {
  return(.h5group_read(
    h5group = x, 
    name = name, 
    transpose = transpose, 
    toS4.func = toS4.func, 
    ...
  ))
}

#' @export
#' @rdname h5Read
#' @method h5Read H5File
h5Read.H5File <- function(
    x, 
    name = NULL,  
    transpose = FALSE, 
    toS4.func = NULL, 
    ...
) {
  name <- h5AbsLinkName(name = name)
  if (identical(x = name, y = "/")) {
    name <- NULL
  }
  return(.h5group_read(
    h5group = x, 
    name = name, 
    transpose = transpose, 
    toS4.func = toS4.func, 
    ...
  ))
}

#' @examples
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' 
#' # Read a matrix
#' x <- h5Read(file, name = "X")
#' x <- h5Read(file, name = "X", transpose = TRUE)
#' x <- h5Read(file, name = "X", idx_list = list(1:10, 1:20))
#' x <- h5Read(
#'   file, 
#'   name = "X", 
#'   idx_list = list(1:10, 1:20), 
#'   transpose = TRUE
#' )
#' 
#' # Read a dgCMatrix
#' x <- h5Read(file, name = "raw/X")
#' x <- h5Read(file, name = "raw/X", transpose = TRUE)
#' 
#' # Read a data.frame
#' x <- h5Read(file, name = "obs")
#' x <- h5Read(file, name = "raw/var") # Read a data.frame with empty column
#' 
#' # Read a list
#' x <- h5Read(file)
#' x <- h5Read(file, "raw")
#' x <- h5Read(file, "obsm")
#' 
#' @export
#' @rdname h5Read
#' @method h5Read character
h5Read.character <- function(
    x, 
    name = NULL,  
    transpose = FALSE, 
    toS4.func = NULL, 
    ...  
) {
  h5fh <- h5TryOpen(filename = x, mode = "r")
  on.exit(expr = h5fh$close())
  return(h5Read(
    x = h5fh, 
    name = name, 
    transpose = transpose, 
    toS4.func = toS4.func,
    ...
  ))
}

#' @export
#' @rdname h5Prep
h5Prep.default <- function(x, ...) {
  return(x)
}

#' @param overwrite Whether or not to overwrite the existing HDF5 link.
#' @param gzip_level Enable zipping at the level given here.
#' 
#' @details
#' By default, \code{h5Write} will try to transform any S4 object \code{x} into 
#' combination of base R objects using \code{\link{h5Prep}} before writting it.
#' 
#' @examples
#' \dontrun{
#' file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
#' tmp.file <- tempfile(fileext = ".h5")
#' h5CreateFile(tmp.file)
#' 
#' # vector -----------------------
#' x <- h5Read(file, "/raw/X/data")
#' h5Write(x, tmp.file, "raw/X/data")
#' x2 <- h5Read(tmp.file, "raw/X/data")
#' stopifnot(identical(x, x2))
#' }
#' 
#' @export
#' @rdname h5Write
h5Write.default <- function(
    x, 
    file, 
    name, 
    overwrite = FALSE, 
    gzip_level = 6,
    ...
) {
  S4_class <- NULL
  if (isS4(x)) {
    S4_class <- as.character(x = class(x = x))
    x <- h5Prep(x = x)
  }
  if (isS4(x)) {
    warning(
      "Writing S4 Class '", class(x = x), "' to HDF5 is not supported, ",
      "skip it."
    )
    return(invisible(x = NULL))
  }
  if (is.vector(x = x)) {
    # Treat vector as 1d array
    .h5write_vector(
      x = x, 
      file = file, 
      name = name, 
      overwrite = overwrite, 
      gzip_level = gzip_level, 
      ...
    )
    return(invisible(x = NULL))
  }
  h5Write(
    x = x, 
    file = file, 
    name = name, 
    overwrite = overwrite, 
    gzip_level = gzip_level, 
    ...
  )
  if (!is.null(x = S4_class)) {
    h5WriteAttr(
      x = file, 
      which = "S4Class",
      robj = S4_class, 
      name = name
    )
  }
  return(invisible(x = NULL))
}

#' @param transpose Whether or not to transpose the input matrix. Only works for 
#' a 2-dimension array-like object.
#' @param block_size Default size for number of columns when \code{transpose} 
#' is \code{TRUE}. 
#' 
#' @examples
#' \dontrun{
#' # matrix -----------------------
#' x <- h5Read(file, "X")
#' h5Write(x, tmp.file, "X")
#' x2 <- h5Read(tmp.file, "X")
#' stopifnot(identical(x, x2))
#' 
#' h5Write(x, tmp.file, "X2", transpose = TRUE)
#' x2 <- h5Read(tmp.file, "X2")
#' stopifnot(identical(t(x), x2))
#' }
#' 
#' @export
#' @rdname h5Write
#' @method h5Write array
h5Write.array <- function(
    x, 
    file, 
    name, 
    overwrite = FALSE, 
    transpose = FALSE, 
    block_size = 5000L,
    gzip_level = 6,
    ...
) {
  if (length(x = x) == 0) {
    return(invisible(x = NULL))
  }
  if (length(x = dim(x = x)) == 1) {
    h5Write(
      x = as.vector(x = x), 
      file = file, 
      name = name, 
      overwrite = overwrite,
      gzip_level = gzip_level,
      ...
    )
    return(invisible(x = NULL))
  }
  name <- h5AbsLinkName(name = name)
  file <- h5Overwrite(file = file, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = file, mode = "r+")
  on.exit(expr = h5fh$close())
  .h5write_array(
    robj = x, 
    h5group = h5fh, 
    name = name, 
    transpose = transpose, 
    block_size = block_size, 
    gzip_level = gzip_level,
    ...
  )
  return(invisible(x = NULL))
}

#' @param ordered When writing a factor, whether or not the categories are 
#' ordered.
#' 
#' @export
#' @rdname h5Write
#' @method h5Write factor
h5Write.factor <- function(
    x,
    file,
    name,
    overwrite = FALSE,
    ordered = TRUE,
    gzip_level = 6,
    ...
){
  if (length(x = x) == 0) {
    return(invisible(x = NULL))
  }
  name <- h5AbsLinkName(name = name)
  file <- h5Overwrite(file = file, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = file, mode = "r+")
  on.exit(expr = h5fh$close())
  .h5write_factor(
    robj = x,
    h5group = h5fh,
    name = name,
    ordered = ordered,
    gzip_level = gzip_level,
    ...
  )
  return(invisible(x = NULL))
}

#' @examples
#' \dontrun{
#' # data.frame -----------------------
#' x <- h5Read(file, "obs")
#' h5Write(x, tmp.file, "obs")
#' x2 <- h5Read(tmp.file, "obs")
#' stopifnot(identical(x, x2))
#' 
#' x <- h5Read(file, "raw/var") # data.frame with empty column
#' h5Write(x, tmp.file, "raw/var")
#' x2 <- h5Read(tmp.file, "raw/var")
#' stopifnot(identical(x, x2))
#' }
#' 
#' @export
#' @rdname h5Write
#' @method h5Write data.frame
h5Write.data.frame <- function(
    x, 
    file, 
    name, 
    overwrite = FALSE, 
    gzip_level = 6,
    ...
) {
  if (nrow(x = x) == 0) {
    return(invisible(x = NULL))
  }
  file <- h5Overwrite(file = file, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = file, mode = "r+")
  on.exit(expr = h5fh$close())
  .h5write_dataframe(
    robj = x, 
    h5group = h5fh, 
    name = name, 
    gzip_level = gzip_level, 
    ...
  )
  return(invisible(x = NULL))
}

#' @param add.shape When writing a CSC- or CSR-matrix, whether or not to also 
#' write the number of dimensions into an HDF5 dataset.
#' @param dimnames When writing a CSC- or CSR-matrix, whether or not to also 
#' write the dimension names.
#' 
#' @examples
#' \dontrun{
#' # dgCMatrix -----------------------
#' x <- h5Read(file, "raw/X")
#' h5Write(x, tmp.file, "raw/X", overwrite = TRUE)
#' x2 <- h5Read(tmp.file, "raw/X")
#' stopifnot(identical(x, x2))
#' }
#' 
#' @importMethodsFrom Matrix t
#' @export
#' @rdname h5Write
#' @method h5Write dgCMatrix
h5Write.dgCMatrix <- function(
    x, 
    file, 
    name, 
    overwrite = FALSE, 
    transpose = FALSE,
    add.shape = FALSE,
    dimnames = list(),
    gzip_level = 6,
    ...
) {
  if (length(x = x) == 0) {
    return(invisible(x = NULL))
  }
  if (transpose) {
    x <- t(x = x)
    gc(verbose = FALSE)
  }
  file <- h5Overwrite(file = file, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = file, mode = "r+")
  on.exit(expr = h5fh$close())
  .h5write_sparse(
    robj = x, 
    h5group = h5fh, 
    name = name, 
    add.shape = add.shape, 
    dimnames = dimnames,
    gzip_level = gzip_level,
    ...
  )
  return(invisible(x = NULL))
}

#' @importFrom MatrixExtra t_shallow
#' @importMethodsFrom Matrix t
#' @export
#' @rdname h5Write
#' @method h5Write dgRMatrix
h5Write.dgRMatrix <- function(
    x, 
    file, 
    name, 
    overwrite = FALSE, 
    transpose = FALSE,
    add.shape = FALSE,
    dimnames = list(),
    gzip_level = 6,
    ...
) {
  if (length(x = x) == 0) {
    return(invisible(x = NULL))
  }
  if (transpose) {
    x <- t(x = x)
    gc(verbose = FALSE)
  }
  x <- t_shallow(x = x)
  return(h5Write(
    x = x,
    file = file,
    name = name,
    overwrite = overwrite,
    transpose = transpose,
    add.shape = add.shape,
    dimnames = dimnames,
    gzip_level = gzip_level,
    ...
  ))
}


#' @examples
#' \dontrun{
#' # list -----------------------
#' x <- h5Read(file)
#' h5Write(x, tmp.file, name = NULL, overwrite = TRUE)
#' x2 <- h5Read(tmp.file)
#' stopifnot(identical(x, x2))
#' }
#' 
#' @export
#' @rdname h5Write
#' @method h5Write list
h5Write.list <- function(
    x, 
    file, 
    name,
    overwrite = FALSE, 
    gzip_level = 6,
    ...
) {
  for (i in seq_along(along.with = x)) {
    if (!any(isValidCharacters(x = names(x = x)[i]))) {
      names(x = x)[i] <- paste0("unnamed", i)
    }
  }
  name <- h5AbsLinkName(name = name)
  file <- h5Overwrite(file = file, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = file, mode = "r+")
  on.exit(expr = h5fh$close())
  
  # Unlike vectors or arrays, creating an empty H5Group for an empty list 
  # does make sense.
  h5CreateGroup(x = h5fh, name = name)
  for (i in seq_along(along.with = x)) {
    h5Write(
      x = x[[i]],
      file = file, 
      name = file.path(name, names(x = x)[i]), 
      overwrite = FALSE,
      gzip_level = gzip_level,
      ...
    )
  }
  h5WriteAttr(x = h5fh, name = name, which = "encoding-type", robj = "dict")
  h5WriteAttr(
    x = h5fh,
    name = name,
    which = "encoding-version",
    robj = "0.1.0"
  )
  return(invisible(x = NULL))
}
