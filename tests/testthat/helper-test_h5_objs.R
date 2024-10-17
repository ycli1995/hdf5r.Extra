
test_h5_info <- function() {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  # h5Exists
  expect_true(h5Exists(file, "/"))
  expect_true(h5Exists(file, "obs"))
  expect_true(h5Exists(file, "X"))
  expect_false(h5Exists(file, "XXX"))
  expect_false(h5Exists(file, "AA/BBB"))
  expect_false(h5Exists(file, "aa/bb/cc/"))
  
  h5fh <- h5TryOpen(file, mode = "r")
  on.exit(h5fh$close_all())
  expect_true(h5Exists(h5fh, "/"))
  expect_true(h5Exists(h5fh, ""))
  expect_false(h5Exists(h5fh, "."))
  expect_false(h5Exists(h5fh, "XXX"))
  expect_false(h5Exists(h5fh, "AA/BBB"))
  expect_false(h5Exists(h5fh, "aa/bb/cc/"))
  
  h5obj <- h5Open(h5fh, "obs")
  expect_true(h5Exists(h5obj, "/"))
  expect_true(h5Exists(h5obj, "/obs"))
  expect_false(h5Exists(h5obj, "."))
  expect_true(h5Exists(h5obj, "groups"))
  expect_true(h5Exists(h5obj, "orig.ident"))
  expect_false(h5Exists(h5obj, "XXX"))
  expect_false(h5Exists(h5obj, "AA/BBB"))
  expect_false(h5Exists(h5obj, "aa/bb/cc/"))
  
  h5obj <- h5Open(h5fh, "uns")
  expect_true(h5Exists(h5obj, "/"))
  expect_false(h5Exists(h5obj, "."))
  expect_false(h5Exists(h5obj, "XXX"))
  expect_false(h5Exists(h5obj, "AA/BBB"))
  expect_false(h5Exists(h5obj, "aa/bb/cc/"))
  
  # dimension
  dim <- c(20, 80)
  expect_equal(h5Dims(file, "X"), dim)
  expect_equal(h5MaxDims(file, "/X"), dim)
  
  h5obj <- h5Open(file, "X", mode = "r")
  expect_equal(h5Dims(h5obj), dim)
  expect_equal(h5MaxDims(h5obj), dim)
  
  dim <- c(19, 80)
  expect_equal(h5Dims(h5fh, "obsm/pca"), dim)
  expect_equal(h5MaxDims(h5fh, "/obsm/pca"), dim)
  
  h5obj <- h5Open(h5fh, "obsm")
  expect_equal(h5Dims(h5obj, "pca"), dim)
  expect_equal(h5MaxDims(h5obj, "/obsm/pca"), dim)
  
  # H5List
  adata_names <- c(
    "X", "layers", "obs", "obsm", "obsp", 
    "raw", "uns", "var", "varm", "varp"
  )
  expect_identical(h5List(file), adata_names)
  expect_identical(h5List(h5fh), adata_names)
  expect_identical(h5List(file, full.names = TRUE), paste0("/", adata_names))
  expect_identical(h5List(h5fh, full.names = TRUE), paste0("/", adata_names))
  
  expect_error(h5List(file, "X"))
  h5obj <- h5Open(file, "X", mode = "r")
  expect_error(h5List(h5obj))
  
  df1 <- h5List(file, simplify = FALSE)
  expect_s3_class(df1, "data.frame")
  expect_equal(df1$name, adata_names)
  
  df1 <- h5List(file, simplify = FALSE, recursive = TRUE)
  expect_equal(df1$name, h5List(file, recursive = TRUE))
  
  reduc_names <- c("pca", "tsne")
  expect_equal(h5List(file, "obsm"), reduc_names)
  h5obj <- h5Open(file, "obsm", mode = "r")
  expect_equal(h5List(h5obj), reduc_names)
}

test_h5group_write_dataframe <- function() {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  h5fh <- h5TryOpen(to.file, mode = "w")
  on.exit(h5fh$close_all())
  
  x <- h5Read(file, "obs")
  h5FileWrite(x, h5fh, name = "/")
  x2 <- h5Read(h5fh)
  expect_identical(x2, x)
  
  h5g <- h5fh$create_group("test")
  h5FileWrite(x, h5g, name = "/")
  x2 <- h5Read(h5g)
  expect_identical(x2, x)
  x2 <- h5Read(h5g, "/")
  expect_identical(x2, x)
  x2 <- h5Read(h5g, "")
  expect_identical(x2, x)
  x2 <- h5Read(h5fh, "/test")
  expect_identical(x2, x)
  
  h5FileWrite(x, h5g, name = "df")
  x2 <- h5Read(h5g, "/df")
  expect_identical(x2, x)
}

test_h5group_write_sparse <- function() {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  h5CreateFile(to.file)
  h5fh <- h5TryOpen(to.file, mode = "r+")
  on.exit(h5fh$close_all())
  
  x <- h5Read(file, "raw/X")
  obs <- h5Read(file, "obs")
  var <- h5Read(file, "raw/var")
  
  h5FileWrite(x, h5fh, name = NULL)
  x2 <- h5Read(h5fh)
  expect_identical(x2, x)
  
  rownames(x) <- rownames(var)
  colnames(x) <- rownames(obs)
  
  h5FileWrite(x, h5fh, name = "/m2")
  x2 <- h5Read(h5fh, name = "m2")
  expect_identical(x2, x)
}

test_h5group_write_list <- function() {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  h5fh <- h5TryOpen(to.file, mode = "w")
  on.exit(h5fh$close_all())
  
  x <- h5Read(file)
  h5FileWrite(x, h5fh, name = "/")
  x2 <- h5Read(h5fh)
  expect_identical(x2, x)
}



