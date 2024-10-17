
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
})

