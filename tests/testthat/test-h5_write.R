
test_that("h5 write sparse matrix", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  tmp.file <- file.path(dir, "test.h5")
  
  ## dgCMatrix
  x <- h5Read(file, "raw/X")
  h5Write(x, tmp.file, "raw/X", overwrite = TRUE)
  x2 <- h5Read(tmp.file, "raw/X")
  expect_identical(x2, x)
  
  obs <- h5Read(file, "obs")
  var <- h5Read(file, "raw/var")
  rownames(x) <- rownames(var)
  colnames(x) <- rownames(obs)
  expect_warning(
    h5Write(x, tmp.file, "/", overwrite = TRUE),
    "will truncate anything in the orignial file"
  )
  x2 <- h5Read(tmp.file, "/")
  expect_identical(x2, x)
  
  h5Write(x, tmp.file, "/raw/t_csc", overwrite = TRUE, transpose = TRUE)
  x2 <- h5Read(tmp.file, "/raw/t_csc")
  expect_identical(x2, t(x))
  
  ## dgRMatrix
  x <- as(x, "RsparseMatrix")
  h5Write(x, tmp.file, "raw/csr", overwrite = TRUE)
  x2 <- h5Read(tmp.file, "/raw/csr")
  expect_identical(x2, x)
  x2 <- h5Read(tmp.file, "/raw/csr", transpose = TRUE)
  expect_identical(t(x2), x)
})

test_that("h5 write matrix", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  tmp.file <- file.path(dir, "test.h5")
  
  x <- h5Read(file, "X")
  h5Write(x, tmp.file, "X", overwrite = TRUE)
  x2 <- h5Read(tmp.file, "/X")
  expect_identical(x2, x)
  
  obs <- h5Read(file, "obs")
  var <- h5Read(file, "var")
  rownames(x) <- rownames(var)
  colnames(x) <- rownames(obs)
  h5Write(x, tmp.file, "/X", overwrite = TRUE)
  x2 <- h5Read(tmp.file, "/X")
  expect_equal(x2, x, ignore_attr = TRUE)
  
  # transpose
  h5Write(x, tmp.file, "/X2", transpose = TRUE, block_size = 10)
  x2 <- h5Read(tmp.file, "/X2")
  expect_equal(t(x2), x, ignore_attr = TRUE)
})

test_that("h5 write list", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  tmp.file <- file.path(dir, "test.h5")
  
  x <- h5Read(file)
  h5Write(x, tmp.file, name = NULL, overwrite = TRUE)
  x2 <- h5Read(tmp.file)
  expect_identical(x, x2)
})

test_that("h5 write data.frame", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  tmp.file <- file.path(dir, "test.h5")
  
  x <- h5Read(file, "/obs")
  h5Write(x, tmp.file, name = NULL, overwrite = TRUE)
  x2 <- h5Read(tmp.file)
  expect_identical(x, x2)
  
  # empty data.frame
  x <- h5Read(file, "/var")
  expect_warning(
    h5Write(x, tmp.file, name = NULL, overwrite = TRUE),
    "will truncate anything in the orignial file"
  )
  x2 <- h5Read(tmp.file)
  expect_identical(x, x2)
})

test_that("h5 write dataset", {
  dir <- withr::local_tempdir()
  tmp.file <- file.path(dir, "test.h5")
  
  expect_error(h5WriteDataset(tmp.file, FALSE, name = "test/bool"))
  
  h5CreateDataset(
    tmp.file, 
    name = "test/bool", 
    dims = 1, 
    storage.mode = logical()
  ) # Must create the dataset first
  h5WriteDataset(tmp.file, FALSE, name = "test/bool")
  x <- h5Read(tmp.file, name = "test/bool")
  expect_false(x)
  
  h5CreateDataset(tmp.file, name = "test/num", dims = 1)
  h5WriteDataset(tmp.file, 100.0, name = "test/num")
  x <- h5Read(tmp.file, name = "test/num")
  expect_equal(x, 100.0)
  
  h5CreateDataset(
    tmp.file, 
    name = "test/string", 
    dims = 1, 
    storage.mode = character()
  )
  h5WriteDataset(tmp.file, "ABC", name = "test/string")
  x <- h5Read(tmp.file, name = "test/string")
  expect_equal(x, "ABC")
  
  # Vector (1d array) ##########
  x1 <- rep(FALSE, 10)
  h5CreateDataset(
    tmp.file, 
    name = "vec/bool", 
    dims = 10, 
    storage.mode = logical()
  )
  h5WriteDataset(tmp.file, x1, name = "vec/bool")
  x <- h5Read(tmp.file, name = "vec/bool")
  expect_identical(x, x1)
  
  x1 <- rep(1.1, 10)
  h5CreateDataset(
    tmp.file, 
    name = "vec/num", 
    dims = 10
  )
  h5WriteDataset(tmp.file, x1, name = "vec/num")
  x <- h5Read(tmp.file, name = "vec/num")
  expect_identical(x, x1)
  
  x1 <- rep(2.0, 5)
  h5WriteDataset(
    tmp.file, 
    x1, 
    name = "vec/num", 
    idx_list = list(c(1, 3, 5, 7, 9)) # Set each indices to be written
  )
  x <- h5Read(tmp.file, name = "vec/num")
  expect_identical(x[c(1, 3, 5, 7, 9)], rep(2.0, 5))
  expect_identical(x[c(2, 4, 6, 8, 10)], rep(1.1, 5))
  
  # matrix ##########
  x1 <- matrix(1.0, 7, 5)
  h5CreateDataset(
    tmp.file, 
    name = "mat/num", 
    dims = dim(x1)
  )
  h5WriteDataset(
    tmp.file, 
    x1, 
    name = "mat/num"
  )
  x <- h5Read(tmp.file, name = "mat/num")
  expect_identical(x, x1)
  
  x1 <- matrix(2.0, 3, 4)
  h5WriteDataset(
    tmp.file, 
    x1, 
    name = "mat/num",
    idx_list = list(2:4, 1:4)
  )
  x <- h5Read(tmp.file, name = "mat/num")
  expect_identical(x[2:4, 1:4], x1)
  
  h5WriteDataset(
    tmp.file, 
    x1, 
    name = "mat/num",
    idx_list = list(1:4, 2:4),  # idx_list must match the transposed matrix
    transpose = TRUE
  )
  x <- h5Read(tmp.file, name = "mat/num")
  expect_identical(x[1:4, 2:4], t(x1))
})


