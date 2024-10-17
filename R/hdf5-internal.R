
.h5_is_a <- function(file, name, what) {
  h5obj <- h5Open(x = file, name = name, mode = "r")
  on.exit(expr = h5obj$close())
  return(inherits(x = h5obj, what = what))
}

.identical_h5loc <- function(x, y) {
  (x$get_filename() == y$get_filename()) & 
    (x$get_obj_name() == y$get_obj_name())
}

## Exclude hdf5 link names =====================================================
.exclude_h5_links <- function(all_links, exclude) {
  exclude <- vapply(
    X = exclude, 
    FUN = h5AbsLinkName, 
    FUN.VALUE = character(length = 1L)
  )
  exclude <- gsub(pattern = "\\/*$", replacement = "", x = exclude)
  keep_links0 <- setdiff(all_links, exclude)
  keep_links <- keep_links0
  while (any(!keep_links %in% "/")) {
    keep_links <- dirname(path = keep_links)
    which.include <- which(x = !keep_links %in% exclude)
    keep_links0 <- keep_links0[which.include]
    keep_links <- keep_links[which.include]
  }
  return(keep_links0)
}

## Low-level helpers for HDF5 attributes =======================================
.h5attr <- function(h5obj, which, ...) {
  if (!h5obj$attr_exists(attr_name = which)) {
    return(NULL)
  }
  attr_obj <- h5obj$attr_open(attr_name = which)
  on.exit(expr = attr_obj$close(), add = TRUE)
  return(tryCatch(expr = attr_obj$read(...), error = function(e) NULL))
}

#' @importFrom hdf5r h5attr_names
.h5attributes <- function(h5obj, ...) {
  attr_names <- h5attr_names(x = h5obj)
  attr_data <- vector(mode = "list", length = length(x = attr_names))
  names(x = attr_data) <- attr_names
  for (i in names(x = attr_data)) {
    attr_data[i] <- list(.h5attr(h5obj = h5obj, which = i, ...))
  }
  return(attr_data)
}

#' @importFrom hdf5r h5garbage_collect
.h5attr_overwrite <- function(h5obj, which, overwrite = TRUE) {
  if (!h5obj$attr_exists_by_name(attr_name = which, obj_name = ".")) {
    return(invisible(x = NULL))
  }
  if (!overwrite) {
    warning(
      "Found attribute that already exists: ",
      "\n  File: ", h5obj$get_filename(), 
      "\n  Object: ", h5obj$get_obj_name(),
      "\n  Attribute: ", which, 
      "\nSet 'overwrite = TRUE' to overwrite it.",
      immediate. = TRUE
    )
    return(invisible(x = NULL))
  }
  h5obj$attr_delete_by_name(attr_name = which, obj_name = ".")
  h5garbage_collect()
  return(invisible(x = NULL))
}

#' @importFrom rlang is_empty is_scalar_atomic
#' @importFrom hdf5r H5S
.h5attr_write <- function(
    h5obj, 
    which, 
    robj, 
    overwrite = TRUE, 
    check.scalar = TRUE,
    stype = c('utf8', 'ascii7')
) {
  stype <- match.arg(arg = stype)
  .h5attr_overwrite(h5obj = h5obj, which = which, overwrite = overwrite)
  h5space <- NULL
  if (is_empty(robj)) {
    h5space <- H5S$new(dims = 0, maxdims = 0)
    on.exit(expr = h5space$close(), add = TRUE)
    h5a <- h5obj$create_attr(
      attr_name = "column-order", 
      dtype = h5GuessDtype(x = robj),
      space = h5space
    )
    return(invisible(x = NULL))
  }
  if (check.scalar && is_scalar_atomic(x = robj)) {
    h5space <- H5S$new(type = "scalar")
    on.exit(expr = h5space$close())
  }
  attr_obj <- h5obj$create_attr_by_name(
    attr_name = which, 
    obj_name = ".", 
    robj = robj,
    space = h5space,
    dtype = h5GuessDtype(x = robj, stype = stype)
  )
  on.exit(expr = attr_obj$close(), add = TRUE)
  return(invisible(x = NULL))
}

#' @importFrom hdf5r h5garbage_collect
.h5attr_delete <- function(h5obj, which) {
  if (h5obj$attr_exists_by_name(attr_name = which, obj_name = ".")) {
    h5obj$attr_delete_by_name(attr_name = which, obj_name = ".")
    h5garbage_collect()
  }
  return(invisible(x = NULL))
}

.h5attr_copy <- function(
    from.h5obj, 
    from.which, 
    to.h5obj, 
    to.which,
    overwrite = TRUE
) {
  if (!from.h5obj$attr_exists_by_name(attr_name = from.which, obj_name = ".")) {
    stop(
      "\nSource attribute doesn't exist:",
      "\n  File: ", from.h5obj$get_filename(),
      "\n  Object: ", from.h5obj$get_obj_name(),
      "\n  Attribute: ", from.which
    )
  }
  is.identical <- .identical_h5loc(x = from.h5obj, y = to.h5obj) &&
    (from.which == to.which)
  if (is.identical) {
    warning(
      "Source attribute is identical to the destination, skip copying: ",
      "\n  File: ", from.h5obj$get_filename(),
      "\n  Object: ", from.h5obj$get_obj_name(),
      "\n  Attribute: ", from.which,
      immediate. = TRUE
    )
    return(invisible(x = NULL))
  }
  .h5attr_overwrite(h5obj = to.h5obj, which = to.which, overwrite = overwrite)
  from.h5a <- from.h5obj$attr_open(attr_name = from.which)
  on.exit(expr = from.h5a$close())
  h5space <- from.h5a$get_space()
  on.exit(expr = h5space$close(), add = TRUE)
  h5dtype <- from.h5a$get_type()
  on.exit(expr = h5dtype$close(), add = TRUE)
  robj <- tryCatch(
    expr = from.h5a$read(),
    error = function(e) NULL
  )
  h5a <- to.h5obj$create_attr_by_name(
    attr_name = to.which, 
    obj_name = ".", 
    robj = robj,
    space = h5space,
    dtype = h5dtype
  )
  on.exit(expr = h5a$close(), add = TRUE)
  return(invisible(x = NULL))
}

#' @importFrom hdf5r h5attr_names
.h5attr_copy_all <- function(
    from.h5fh,
    from.name,
    to.h5fh,
    to.name,
    overwrite = TRUE
) {
  from.h5obj <- h5Open(x = from.h5fh, name = from.name)
  if (!identical(x = from.h5obj, y = from.h5fh)) {
    on.exit(expr = from.h5obj$close())
  }
  to.h5obj <- h5Open(x = to.h5fh, name = to.name)
  if (!identical(x = to.h5obj, y = to.h5fh)) {
    on.exit(expr = to.h5obj$close(), add = TRUE)
  }
  all.attrs <- h5attr_names(x = from.h5obj)
  for (i in all.attrs) {
    .h5attr_copy(
      from.h5obj = from.h5obj,
      to.h5obj = to.h5obj,
      from.which = i,
      to.which = i,
      overwrite = overwrite
    )
  }
  return(invisible(x = NULL))
}

## H5 copy or delete ===========================================================

.h5copy_same_file <- function(
    h5.file, 
    from.name, 
    to.name, 
    overwrite = FALSE, 
    verbose = TRUE,
    ...
) {
  if (from.name == to.name) {
    warning(
      "The source object is identical to the destination, skip copying.",
      immediate. = TRUE
    )
    return(invisible(x = NULL))
  }
  if (to.name == "/") {
    stop("\nCannot copy object to '/' within an H5 file.")
  }
  h5fh <- h5TryOpen(filename = h5.file, mode = "r+")
  on.exit(expr = h5fh$close())
  if (h5Exists(x = h5fh, name = to.name)) {
    if (!overwrite) {
      warning(
        "Destination object already exists. ",
        "Set 'overwrite = TRUE' to remove it.",
        immediate. = TRUE
      )
      return(invisible(x = NULL))
    }
    verboseMsg("Destination object already exists, removing it.")
    h5fh$link_delete(name = to.name)
  }
  h5CreateGroup(
    x = h5fh, 
    name = dirname(path = to.name), 
    show.warnings = FALSE
  )
  h5fh$obj_copy_from(
    src_loc = h5fh, 
    src_name = from.name, 
    dst_name = to.name, 
    ...
  )
  .h5attr_copy_all(
    from.h5fh = h5fh,
    from.name = from.name,
    to.h5fh = h5fh,
    to.name = to.name,
    overwrite = TRUE
  )
  return(invisible(x = NULL))
}

.h5copy_different_file <- function(
    from.file,
    from.name,
    to.file,
    to.name,
    overwrite = FALSE,
    verbose = TRUE,
    ...
) {
  if (to.name != "/") {
    to.h5fh <- h5TryOpen(filename = to.file, mode = "a")
    if (h5Exists(x = to.h5fh, name = to.name)) {
      if (!overwrite) {
        warning(
          "Destination object already exists. ",
          "Set 'overwrite = TRUE' to remove it.",
          immediate. = TRUE
        )
        return(invisible(x = NULL))
      }
      verboseMsg("Destination object already exists, removing it.")
      to.h5fh$link_delete(name = to.name)
    }
  } else {
    if (file.exists(to.file) && !overwrite) {
      warning(
        "Destination file already exists. ",
        "Set 'overwrite = TRUE' to overwrite the '/' link for it.",
        immediate. = TRUE
      )
      return(invisible(x = NULL))
    }
    if (from.name == "/") {
      verboseMsg("Copy the source file directly.")
      file.copy(from = from.file, to = to.file, overwrite = TRUE)
      return(invisible(x = NULL))
    }
    to.h5fh <- h5TryOpen(filename = to.file, mode = "w")
  }
  on.exit(expr = to.h5fh$close())
  h5fh <- h5TryOpen(filename = from.file, mode = "r")
  on.exit(expr = h5fh$close(), add = TRUE)
  if (!h5Exists(x = h5fh, name = from.name)) {
    stop("\nSource object doesn't exist")
  }
  h5CreateGroup(
    x = to.h5fh, 
    name = dirname(path = to.name), 
    show.warnings = FALSE
  )
  to.h5fh$obj_copy_from(
    src_loc = h5fh, 
    src_name = from.name, 
    dst_name = to.name, 
    ...
  )
  .h5attr_copy_all(
    from.h5fh = h5fh,
    from.name = from.name,
    to.h5fh = to.h5fh,
    to.name = to.name,
    overwrite = TRUE
  )
  return(invisible(x = NULL))
}

.h5delete <- function(h5obj, name, verbose = TRUE, ...) {
  verboseMsg(
    "Deleting an H5 object:",
    "\n  File: ", h5obj$get_filename(),
    "\n  From: ", h5obj$get_obj_name(),
    "\n  Object: ", name
  )
  if (!h5Exists(x = h5obj, name = name)) {
    warning("The H5 object to be deleted doesn't exists.", immediate. = TRUE)
    return(invisible(x = NULL))
  }
  h5obj$link_delete(name = name, ...)
  return(invisible(x = NULL))
}

## H5 list =====================================================================
.h5list <- function(
    h5obj,
    recursive = FALSE,
    full.names = FALSE,
    simplify = TRUE,
    detailed = FALSE,
    ...
) {
  df <- h5obj$ls(recursive = recursive, detailed = detailed, ...)
  if (full.names) {
    df$name <- paste0(h5obj$get_obj_name(), "/", df$name)
    df$name <- gsub(pattern = "^/+", replacement = "/", x = df$name)
  }
  if (simplify) {
    return(df$name)
  }
  return(df)
}

## H5 create group and dataset =================================================
.h5group_create_group <- function(h5group, name, show.warnings = TRUE, ...) {
  name <- unlist(x = strsplit(x = name, split = '/', fixed = TRUE))
  name <- Filter(f = nchar, x = name)
  if (length(x = name) == 0) {
    return(invisible(x = NULL))
  }
  path <- "."
  file_warning <- paste0("\n  File: ", h5group$get_filename())
  group_warning <- paste0("\n  Source group: ", h5group$get_obj_name())
  for (i in name) {
    path <- paste0(path, "/", i)
    if (!h5group$exists(name = path)) {
      h5g <- h5group$create_group(name = path, ...)
      h5g$close()
      next
    }
    if (show.warnings) {
      warning(
        "H5 group already exists: ", file_warning, group_warning,
        "\n  Target group: ", path,
        immediate. = TRUE
      )
    }
  }
  return(invisible(x = NULL))
}

#' @importFrom hdf5r guess_chunks H5P_DATASET_CREATE H5S
.h5group_create_dataset <- function(
    h5group, 
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
  dtype <- dtype %||% h5GuessDtype(x = storage.mode, stype = stype)
  if (!inherits(x = dtype, what = "H5T")) {
    stop("\n  'dtype' must be an 'H5T'")
  }
  maxdims <- maxdims %||% dims
  h5space <- H5S$new(dims = dims, maxdims = maxdims)
  on.exit(expr = h5space$close(), add = TRUE)
  h5d_create_pl <- H5P_DATASET_CREATE$new()
  on.exit(expr = h5d_create_pl$close(), add = TRUE)
  if (gzip_level > 0) {
    h5d_create_pl$set_shuffle()
  }
  if (any(chunk_size %in% "auto")) {
    chunk_size <- guess_chunks(
      space_maxdims = maxdims,
      dtype_size = dtype$get_size(variable_as_inf = FALSE)
    )
  }
  .h5group_create_group(
    h5group = h5group, 
    name = dirname(path = name), 
    show.warnings = FALSE
  )
  h5dataset <- h5group$create_dataset(
    name = name, 
    dtype = dtype,
    space = h5space,
    dataset_create_pl = h5d_create_pl, 
    chunk_dims = chunk_size,
    gzip_level = gzip_level,
    ...
  )
  on.exit(expr = h5dataset$close(), add = TRUE)
  return(invisible(x = NULL))
}

#' @importFrom hdf5r H5P_DATASET_CREATE H5S
#' @importFrom checkmate assert_scalar
.h5group_write_scalar <- function(
    h5group, 
    name, 
    robj, 
    stype = c('utf8', 'ascii7'),
    ...
) {
  stype <- match.arg(arg = stype)
  assert_scalar(x = robj)
  dtype <- h5GuessDtype(x = robj, stype = stype)
  .h5group_create_group(
    h5group = h5group, 
    name = dirname(path = name),
    show.warnings = FALSE
  )
  h5space <- H5S$new(type = "scalar")
  on.exit(expr = h5space$close(), add = TRUE)
  h5d <- h5group$create_dataset(
    name = name, 
    dtype = dtype,
    space = h5space,
    chunk_dims = NULL,
    ...
  )
  on.exit(expr = h5d$close(), add = TRUE)
  h5d$write(args = NULL, value = robj)
  ## Add encoding informations for scalar
  .h5attr_write(
    h5obj = h5d, 
    which = "encoding-version", 
    robj = "0.2.0", 
    overwrite = FALSE
  )
  encoding_type <- "numeric-scalar"
  if (is.character(x = robj)) {
    encoding_type <- "string"
  }
  .h5attr_write(
    h5obj = h5d, 
    which = "encoding-type", 
    robj = encoding_type, 
    overwrite = FALSE
  )
  return(invisible(x = NULL))
}

.h5group_write_dataset <- function(
    h5group,
    robj,
    name,
    idx_list = NULL,
    transpose = FALSE,
    block_size = 5000L,
    verbose = TRUE,
    ...
) {
  h5d <- h5Open(x = h5group, name = name)
  if (!identical(x = h5d, y = h5group)) {
    on.exit(expr = h5d$close())
  }
  if (!inherits(x = h5d, "H5D")) {
    stop("\n  '", h5d$get_obj_name(), "' is not an H5D")
  }
  return(h5WriteDataset(
    x = h5d,
    robj = robj,
    idx_list = idx_list,
    transpose = transpose, 
    block_size = block_size,
    verbose = verbose,
    ...
  ))
}

.h5group_read_dataset <- function(
    h5group, 
    name, 
    idx_list = NULL, 
    transpose = FALSE, 
    ...
) {
  h5d <- h5Open(x = h5group, name = name)
  if (!identical(x = h5d, y = h5group)) {
    on.exit(expr = h5d$close())
  }
  if (!inherits(x = h5d, what = "H5D")) {
    stop("\n  '", name, "' is not an H5D")
  }
  return(h5ReadDataset(
    x = h5d, 
    transpose = transpose, 
    idx_list = idx_list, 
    ...
  ))
}

.h5group_read <- function(
    h5group, 
    name = NULL, 
    transpose = FALSE, 
    toS4.func = NULL, 
    ...
) {
  if (!is.null(x = name)) {
    h5obj <- h5Open(x = h5group, name = name)
    if (!identical(x = h5obj, y = h5group)) {
      on.exit(expr = h5obj$close())
    }
    if (inherits(x = h5obj, what = "H5D")) {
      return(h5ReadDataset(x = h5obj, transpose = transpose))
    }
    return(h5Read(
      x = h5obj, 
      transpose = transpose, 
      toS4.func = toS4.func, 
      ...
    ))
  }
  encoding_type <- h5Attr(x = h5group, which = "encoding-type")
  encoding_type <- encoding_type %||% ""
  r_obj <- switch(
    EXPR = encoding_type,
    "categorical" = .h5read_factor(h5obj = h5group),
    "dataframe" = .h5read_dataframe(h5obj = h5group),
    "csr_matrix" = .h5read_sparse(h5obj = h5group, transpose = transpose),
    "csc_matrix" = .h5read_sparse(h5obj = h5group, transpose = transpose),
    "nullable-boolean" = .h5read_nullable(h5obj = h5group),
    "nullable-integer" = .h5read_nullable(h5obj = h5group),
    .h5read_list(h5obj = h5group, transpose = transpose, ...)
  )
  if (is.null(x = toS4.func)) {
    return(r_obj)
  }
  if (!is.function(x = toS4.func)) {
    stop("\n  'toS4.func' must be NULL or a function.")
  }
  S4Class <- h5Attr(x = h5group, which = "S4Class")
  return(toS4.func(r_obj, S4Class))
}


## Assertive helpers ===========================================================
#' @importFrom hdf5r H5D
.assert_h5d <- function(h5obj) {
  if (!inherits(x = h5obj, what = "H5D")) {
    stop(
      "\nNot an H5D:",
      "\n  File: ", h5obj$get_filename(),
      "\n  Object: ", h5obj$get_obj_name()
    )
  }
  return(invisible(x = NULL))
}

## Helpers for handling operations by chunks ===================================
.idx_list_to_matrix <- function(idx_list) {
  return(vapply(
    X = idx_list, 
    FUN = function(x) as.integer(x = c(min(x), max(x))), 
    FUN.VALUE = integer(length = 2L)
  ))
}

.idx_list_to_msg <- function(idx_list) {
  msg <- apply(
    X = .idx_list_to_matrix(idx_list = idx_list), 
    MARGIN = 2, 
    FUN = function(x) paste0(x[1], ":", x[2]), 
    simplify = TRUE
  )
  return(paste0("(", paste(msg, collapse = ", "), ")"))
}

## Write R array-like data to an existing H5 dataset ===========================
.get_valid_dims <- function(x, transpose = FALSE) {
  dims <- dim(x = x)
  dims <- dims %||% length(x = x)
  if (transpose) {
    if (!length(x = dims) == 2) {
      stop("'transpose' only works for matrix-like data")
    }
    dims <- rev(x = dims)
  }
  return(dims)
}

.check_before_h5d_write <- function(h5obj, dims, idx_list = NULL) {
  .assert_h5d(h5obj = h5obj)
  maxdims <- h5obj$maxdims
  n.dims <- length(x = dims)
  n.h5dims <- length(x = maxdims)
  name <- h5obj$get_obj_name()
  if (is.null(x = idx_list)) {
    if (n.dims != n.h5dims) {
      stop(
        "\nDimension number doesn't match: ",
        "\n  Input R object: ", n.dims,
        "\n  H5 dataset: ", n.h5dims,
        "\n  Destination H5 dataset: ", name
      )
    }
    for (i in seq_along(along.with = dims)) {
      if (dims[i] > maxdims[i]) {
        stop(
          "\nSubscript out of bounds: ",
          "\n  Input dims[[", i, "]]: ", dims[i],
          "\n  H5 dataset max dims[", i, "]: ", maxdims[i],
          "\n  Destination H5 dataset: ", name
        )
      }
    }
    return(invisible(x = NULL))
  }
  if (!is.list(x = idx_list)) {
    stop("\n  'idx_list' must be either NULL or a list")
  }
  if (length(x = idx_list) != n.h5dims) {
    stop(
      "\nElement number of 'idx_list' doesn't match ",
      "the dimension number of dataset:",
      "\n  idx_list: ", length(x = idx_list),
      "\n  H5 dataset: ", n.h5dims,
      "\n  Destination H5 dataset: ", name
    )
  }
  max.idx <- vapply(
    X = idx_list, 
    FUN = function(x) as.integer(x = max(x)), 
    FUN.VALUE = integer(length = 1)
  )
  len.idx <- lengths(x = idx_list)
  if (any(dims != len.idx)) {
    stop(
      "\n'idx_list' doesn't match the dimensions of 'robj':",
      "\n  idx_list: ", paste(len.idx, collapse = ", "),
      "\n  robj: ", paste(dims, collapse = ", ")
    )
  }
  for (i in seq_along(along.with = maxdims)) {
    if (max.idx[i] > maxdims[i]) {
      stop(
        "\nSubscript out of bounds: ",
        "\n  Max idx_list[[", i, "]]: ", max.idx[i],
        "\n  H5 dataset max dims[", i, "]: ", maxdims[i],
        "\n  Destination H5 dataset: ", name
      )
    }
    if (len.idx[i] > maxdims[i]) {
      stop(
        "\nLength out of bounds: ",
        "\n  Length of idx_list[[", i, "]]: ", len.idx[i],
        "\n  H5 dataset max dims[", i, "]: ", maxdims[i],
        "\n  Destination H5 dataset: ", name
      )
    }
  }
  return(invisible(x = NULL))
}

.h5d_set_encode <- function(robj, h5d, ...) {
  type.x <- typeof(x = robj)
  .h5attr_write(h5obj = h5d, which = "encoding-version", robj = "0.2.0")
  encoding_type <- "array"
  if (is.character(x = robj)) {
    encoding_type <- "string-array"
  }
  .h5attr_write(h5obj = h5d, which = "encoding-type", robj = encoding_type)
  return(invisible(x = NULL))
}

.h5write_array <- function(
    robj,
    h5group,
    name,
    transpose = FALSE, 
    block_size = 5000L,
    maxdims = NULL,
    stype = c('utf8', 'ascii7'),
    chunk_size = "auto",
    gzip_level = 6,
    ...
) {
  stype <- match.arg(arg = stype)
  dims <- .get_valid_dims(x = robj, transpose = transpose)
  h5CreateDataset(
    x = h5group,
    name = name,
    dtype = NULL,
    storage.mode = robj[1],
    stype = stype,
    dims = dims,
    maxdims = maxdims,
    chunk_size = chunk_size,
    gzip_level = gzip_level,
    ...
  )
  h5d <- h5Open(x = h5group, name = name)
  on.exit(expr = h5d$close(), add = TRUE)
  idx_list <- NULL
  if (transpose) {
    idx_list <- lapply(X = dims, FUN = seq_len)
  }
  .check_before_h5d_write(h5obj = h5d, dims = dims, idx_list = idx_list)
  h5WriteDataset(
    x = h5d, 
    robj = robj, 
    idx_list = idx_list,
    transpose = transpose, 
    block_size = block_size
  )
  .h5d_set_encode(robj = robj, h5d = h5d)
  return(invisible(x = NULL))
}

.h5group_write_vector <- function(
    robj, 
    h5group, 
    name, 
    stype = c('utf8', 'ascii7'), 
    ...
) {
  if (length(x = robj) == 0) {
    return(invisible(x = NULL))
  }
  stype <- match.arg(arg = stype)
  dims <- .get_valid_dims(x = robj, transpose = FALSE)
  h5CreateDataset(
    x = h5group,
    name = name,
    dtype = NULL,
    storage.mode = robj[1],
    stype = stype,
    dims = dims,
    ...
  )
  h5d <- h5Open(x = h5group, name = name)
  on.exit(expr = h5d$close(), add = TRUE)
  h5d$write(args = NULL, value = as.array(x = robj))
  .h5d_set_encode(robj = robj, h5d = h5d)
  return(invisible(x = NULL))
}

.h5write_vector <- function(
    x, 
    file, 
    name, 
    overwrite = FALSE, 
    gzip_level = 6,
    ...
) {
  if (length(x = x) == 0) {
    return(invisible(x = NULL))
  }
  name <- h5AbsLinkName(name = name)
  file <- h5Overwrite(file = file, name = name, overwrite = overwrite)
  h5fh <- h5TryOpen(filename = file, mode = "r+")
  on.exit(expr = h5fh$close())
  # .h5write_array(
  #   robj = x, 
  #   h5group = h5fh, 
  #   name = name, 
  #   transpose = FALSE, 
  #   gzip_level = gzip_level,
  #   ...
  # )
  .h5group_write_vector(
    robj = x,
    h5group = h5fh,
    name = name,
    gzip_level = gzip_level,
    ...
  )
  return(invisible(x = NULL))
}

#' @importFrom rlang .data
.h5write_factor <- function(
    robj, 
    h5group, 
    name, 
    ordered = TRUE, 
    gzip_level = 6, 
    ...
) {
  r_obj <- list(
    codes = as.integer(x = robj) - 1L %>% replace(is.na(.), -1L),
    categories = levels(x = robj)
  )
  for (i in names(x = r_obj)) {
    .h5group_write_vector(
      robj = r_obj[[i]],
      h5group = h5group,
      name = file.path(name, i),
      gzip_level = gzip_level,
      ...
    )
  }
  h5WriteAttr(x = h5group, name = name, which = "ordered", robj = ordered)
  h5WriteAttr(
    x = h5group, 
    name = name, 
    which = "encoding-type", 
    robj = "categorical"
  )
  h5WriteAttr(
    x = h5group, 
    name = name,
    which = "encoding-version",
    robj = "0.2.0"
  )
  return(invisible(x = NULL))
}

#' @importFrom hdf5r H5S
.h5write_dataframe <- function(
    robj, 
    h5group, 
    name, 
    gzip_level = 6, 
    ...
) {
  .h5group_write_vector(
    robj = rownames(x = robj), 
    h5group = h5group, 
    name = file.path(name, "_index"), 
    gzip_level = gzip_level,
    ...
  )
  h5WriteAttr(
    x = h5group, 
    name = name, 
    which = "encoding-type",
    robj = "dataframe"
  )
  h5WriteAttr(
    x = h5group, 
    name = name, 
    which = "encoding-version",
    robj = "0.2.0"
  )
  h5WriteAttr(
    x = h5group, 
    name = name, 
    which = "_index",
    robj = "_index"
  )
  if (ncol(x = robj) == 0) {
    ## Must add an empty 'column-order' to match anndata's IOSpec
    h5obj <- h5group$open(name = name)
    on.exit(expr = h5obj$close())
    h5space <- H5S$new(dims = 0, maxdims = 0)
    on.exit(expr = h5space$close(), add = TRUE)
    h5a <- h5obj$create_attr(
      attr_name = "column-order", 
      dtype = h5GuessDtype(x = 0.1),
      space = h5space
    )
    on.exit(expr = h5a$close(), add = TRUE)
    return(invisible(x = NULL))
  }
  for (i in colnames(x = robj)) {
    if (is.factor(x = robj[, i, drop = TRUE])) {
      .h5write_factor(
        robj = robj[, i, drop = TRUE],
        h5group = h5group,
        name = file.path(name, i),
        gzip_level = gzip_level,
        ...
      )
      next
    }
    .h5write_array(
      robj = robj[, i, drop = TRUE],
      h5group = h5group,
      name = file.path(name, i),
      transpose = FALSE,
      gzip_level = gzip_level,
      ...
    )
  }
  h5WriteAttr(
    x = h5group, 
    name = name, 
    which = "column-order",
    robj = colnames(x = robj),
    check.scalar = FALSE
  )
  return(invisible(x = NULL))
}

#' @importClassesFrom Matrix dgRMatrix
.h5write_sparse <- function(
    robj, 
    h5group, 
    name, 
    add.shape = FALSE, 
    dimnames = list(),
    gzip_level = gzip_level,
    ...
) {
  h5d_sparse_names <- c(x = "data", i = "indices", p = "indptr")
  if (is(object = robj, class2 = "dgRMatrix")) {
    h5d_sparse_names <- c(x = "data", j = "indices", p = "indptr")
  }
  for (i in names(x = h5d_sparse_names)) {
    .h5write_array(
      robj = slot(object = robj, name = i),
      h5group = h5group,
      name = file.path(name, h5d_sparse_names[i]),
      transpose = FALSE,
      maxdims = Inf,
      gzip_level = gzip_level,
      ...
    )
  }
  .h5_set_sparse_attrs(
    robj = robj, 
    h5group = h5group, 
    name = name, 
    add.shape = add.shape, 
    dimnames = dimnames
  )
  return(invisible(x = NULL))
}

.h5_set_sparse_attrs <- function(
    robj,
    h5group, 
    name,
    add.shape = FALSE, 
    dimnames = list()
) {
  h5WriteAttr(
    x = h5group,
    name = name, 
    which = "shape",
    robj = rev(x = dim(x = robj))
  )
  encode_type <- if (is(object = robj, class2 = "dgCMatrix")) {
    "csr_matrix"
  } else if (is(object = robj, class2 = "dgRMatrix")) {
    "csc_matrix"
  }
  h5WriteAttr(
    x = h5group,
    name = name, 
    which = "encoding-type",
    robj = encode_type
  )
  h5WriteAttr(
    x = h5group,
    name = name, 
    which = "encoding-version",
    robj = "0.1.0"
  )
  if (add.shape) {
    .h5write_array(
      robj = rev(x = dim(x = robj)), 
      h5group = h5group, 
      name = file.path(name, "shape"), 
      transpose = FALSE
    )
  }
  if (identical(x = lengths(x = dimnames), y = dim(x = robj))) {
    names(x = dimnames) <- c("row_names", "col_names")
    for (i in names(x = dimnames)) {
      .h5write_array(
        robj = dimnames[[i]], 
        h5group = h5group, 
        name = file.path(name, i),
        transpose = FALSE
      )
    }
  }
  return(invisible(x = NULL))
}

## Read H5 Dataset =============================================================
.check_before_h5d_read <- function(h5obj, idx_list = NULL) {
  .assert_h5d(h5obj = h5obj)
  name <- h5obj$get_obj_name()
  if (is.null(x = idx_list)) {
    return(invisible(x = NULL))
  }
  if (!is.list(x = idx_list)) {
    stop("\n  'idx_list' must be either NULL or a list")
  }
  dims <- h5obj$dims
  if (length(x = idx_list) != length(x = dims)) {
    stop(
      "\nElement number of 'idx_list' doesn't match ",
      "the dimension number of dataset:",
      "\n  idx_list: ", length(x = idx_list),
      "\n  H5 dataset: ", dims,
      "\n  Destination H5 dataset: ", name
    )
  }
  # Unlike writing, we only need to check the maximun of idx_list while reading
  max.idx <- vapply(
    X = idx_list, 
    FUN = max, 
    FUN.VALUE = integer(length = 1)
  )
  for (i in seq_along(along.with = dims)) {
    if (max.idx[i] > dims[i]) {
      stop(
        "\nSubscript out of bounds: ",
        "\n  Max idx_list[[", i, "]]: ", max.idx[i],
        "\n  H5 dataset dims[", i, "]: ", dims[i],
        "\n  Destination H5 dataset: ", name
      )
    }
  }
  return(invisible(x = NULL))
}

#' @importFrom hdf5r h5garbage_collect
.h5read_factor <- function(h5obj) {
  h5codes <- h5obj$open(name = 'codes')
  on.exit(expr = h5codes$close(), add = TRUE)
  h5categories <- h5obj$open(name = 'categories')
  on.exit(expr = h5categories$close(), add = TRUE)
  codes <- h5codes$read()
  categories <- h5categories$read()
  h5garbage_collect()
  r_obj <- factor(x = categories[codes + 1L], levels = categories)
  return(r_obj)
}

.h5read_list <- function(h5obj, transpose = FALSE, ...) {
  r_obj <- list()
  elem_names <- h5obj$names
  if (length(x = elem_names) == 0) {
    return(r_obj)
  }
  for (i in elem_names) {
    r_obj[[i]] <- h5Read(x = h5obj, name = i, transpose = transpose, ...)
  }
  return(r_obj)
}

.h5get_column_order <- function(h5obj) {
  h5a <- h5obj$attr_open(attr_name = "column-order")
  on.exit(expr = h5a$close())
  h5space <- h5a$get_space()
  on.exit(expr = h5space$close(), add = TRUE)
  if (h5space$dims == 0) {
    return(invisible(x = NULL))
  }
  return(h5a$read())
}

.h5read_dataframe <- function(h5obj) {
  col_orders <- .h5get_column_order(h5obj = h5obj)
  index <- h5Attr(x = h5obj, which = "_index")
  r_list <- .h5read_list(h5obj = h5obj)
  rownames <- NULL
  if (length(x = index) > 0) {
    stopifnot(length(x = index) == 1)
    rownames <- r_list[[index]]
    r_list[[index]] <- NULL
  }
  if (!is.null(x = col_orders)) {
    r_list <- r_list[col_orders]
  }
  if (length(x = r_list) == 0) {
    return(data.frame(row.names = rownames, stringsAsFactors = FALSE))
  }
  # Check nrows for data.frame
  len <- lengths(x = r_list)
  nrows <- max(len)
  rm_names <- names(x = r_list)[len != nrows]
  if (length(x = rm_names) > 0) {
    warning(
      "Remove columns whose length is not ", nrows, ": \n  ",
      paste(rm_names, collapse = ", "),
      immediate. = TRUE, call. = FALSE
    )
    r_list <- r_list[len == nrows]
  }
  return(data.frame(r_list, row.names = rownames, stringsAsFactors = FALSE))
}

#' @importFrom Matrix sparseMatrix
#' @importFrom hdf5r h5garbage_collect
#' @importMethodsFrom Matrix t
.h5read_sparse <- function(h5obj, transpose = FALSE) {
  dims <- h5Attr(x = h5obj, which = "shape")
  h5data <- h5obj$open(name = "data")
  on.exit(expr = h5data$close(), add = TRUE)
  h5indices <- h5obj$open(name = "indices")
  on.exit(expr = h5indices$close(), add = TRUE)
  h5indptr <- h5obj$open(name = "indptr")
  on.exit(expr = h5indptr$close(), add = TRUE)
  encode <- h5Attr(x = h5obj, which = "encoding-type")
  m <- switch(
    EXPR = encode,
    "csr_matrix" = sparseMatrix(
      x = h5data$read(),
      i = h5indices$read(),
      p = h5indptr$read(),
      dims = rev(x = dims),
      index1 = FALSE,
      repr = "C"
    ),
    "csc_matrix" = sparseMatrix(
      x = h5data$read(),
      j = h5indices$read(),
      p = h5indptr$read(),
      dims = rev(x = dims),
      index1 = FALSE,
      repr = "R"
    )
  )
  h5garbage_collect()
  if (h5Exists(x = h5obj, name = "row_names")) {
    rownames(x = m) <- h5ReadDataset(x = h5obj, name = "row_names")
  }
  if (h5Exists(x = h5obj, name = "col_names")) {
    colnames(x = m) <- h5ReadDataset(x = h5obj, name = "col_names")
  }
  if (transpose) {
    m <- t(x = m)
  }
  return(m)
}

.h5read_nullable <- function(h5obj) {
  h5values <- h5obj$open(name = 'values')
  on.exit(expr = h5values$close(), add = TRUE)
  h5mask <- h5obj$open(name = 'mask')
  on.exit(expr = h5mask$close(), add = TRUE)
  values <- h5values$read()
  mask <- h5mask$read()
  values[mask] <- NA
  return(values)
}
