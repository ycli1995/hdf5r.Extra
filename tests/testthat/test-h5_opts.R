
test_that("h5 open", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  h5fh <- h5TryOpen(file, mode = "r")
  expect_s3_class(h5fh, "H5File")
  
  h5obj <- h5Open(file, "/", mode = "r")
  expect_s3_class(h5obj, "H5File")
  h5obj <- h5Open(file, "X", mode = "r")
  expect_s3_class(h5obj, "H5D")
  h5obj <- h5Open(file, "obs", mode = "r")
  expect_s3_class(h5obj, "H5Group")
  
  h5obj <- h5Open(h5fh, "/")
  expect_s3_class(h5obj, "H5File")
  h5obj <- h5Open(h5fh, "obs")
  expect_s3_class(h5obj, "H5Group")
  h5obj <- h5Open(h5fh, "/obs")
  expect_s3_class(h5obj, "H5Group")
  
  h5g <- h5Open(h5fh, "obs")
  expect_error(h5obj <- h5Open(h5g, "/"))
  expect_identical(h5obj$get_obj_name(), h5g$get_obj_name())
  h5g2 <- h5Open(h5g, "/obs")
  expect_identical(h5g$get_obj_name(), h5g2$get_obj_name())
})

# test_that("h5 information", {
#   test_h5_info()
# })

test_that("h5 delete", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  file.copy(file, to.file)
  
  expect_error(h5Delete(to.file, "/", verbose = FALSE))
  
  expect_true(h5Exists(to.file, "obs"))
  h5Delete(to.file, "obs", verbose = FALSE)
  expect_false(h5Exists(to.file, "obs"))
  
  expect_warning(
    h5Delete(to.file, "xxxx", verbose = FALSE), 
    "to be deleted doesn't exists."
  )
  
  h5fh <- h5TryOpen(to.file, mode = "r+")
  expect_error(h5Delete(h5fh, "/", verbose = FALSE))
  expect_true(h5Exists(h5fh, "uns"))
  h5Delete(h5fh, "uns", verbose = FALSE)
  expect_false(h5Exists(h5fh, "uns"))
  
  h5g <- h5Open(h5fh, "obsm")
  expect_error(h5Delete(h5g, "/", verbose = FALSE))
  expect_warning(
    h5Delete(h5g, "", verbose = FALSE),
    "to be deleted doesn't exists."
  )
  expect_true(h5Exists(h5g, "pca"))
  h5Delete(h5g, "pca", verbose = FALSE)
  expect_false(h5Exists(h5g, "pca"))
  
  expect_true(h5Exists(h5g, "/X"))
  h5Delete(h5g, "/X", verbose = FALSE)
  expect_false(h5Exists(h5fh, "/X"))
})

test_that("h5 helpers", {
  expect_identical(h5AbsLinkName("ggg"), "/ggg")
  expect_identical(h5AbsLinkName("ggg/ddd"), "/ggg/ddd")
  expect_identical(h5AbsLinkName("ggg///ddd"), "/ggg/ddd")
  expect_identical(h5AbsLinkName(NA), "/")
  expect_identical(h5AbsLinkName(""), "/")
  expect_identical(h5AbsLinkName(NULL), "/")
  
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  expect_identical(h5Class(file, "X"), "H5D")
  expect_identical(h5Class(file, "/obsm/pca"), "H5D")
  expect_identical(h5Class(file, "/raw/X"), "H5Group")
  expect_identical(h5Class(file, "/"), "H5File")
  
  expect_true(is.H5D(file, "X"))
  expect_true(is.H5D(file, "/obsm/pca"))
  expect_false(is.H5D(file, "/raw/X"))
  
  expect_true(is.H5Group(file, "obs"))
  expect_true(is.H5Group(file, "/raw/X"))
  expect_false(is.H5Group(file, "X"))
})

test_that("h5 copy", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  
  h5Copy(file, "obs", to.file, "obs", verbose = FALSE)
  obs <- h5Read(file, "obs")
  obs2 <- h5Read(to.file, "obs")
  expect_identical(obs2, obs)
  
  h5Copy(file, "obsm/tsne", to.file, "obsm/tsne", verbose = FALSE)
  h5Copy(file, "obsm/pca", to.file, "obsm/pca", verbose = FALSE)
  obsm <- h5Read(file, "obsm")
  obsm2 <- h5Read(to.file, "obsm")
  expect_identical(obsm2$pca, obsm$pca)
  expect_identical(obsm2$tsne, obsm$tsne)
  
  x <- h5Read(file)
  h5Copy(file, "/", to.file, "/", overwrite = TRUE, verbose = FALSE)
  x2 <- h5Read(to.file)
  expect_identical(x2, x)
})

test_that("h5 move", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  file.copy(file, to.file)
  
  obs <- h5Read(to.file, "obs")
  h5Move(to.file, "obs", "obs2", verbose = FALSE)
  obs2 <- h5Read(to.file, "obs2")
  expect_identical(obs2, obs)
  expect_false(h5Exists(to.file, "obs"))
  
  # Move an object to an existing link
  expect_warning(
    h5Move(to.file, "obs2", "var", verbose = FALSE),
    "Destination object already exists."
  )
  h5Move(to.file, "obs2", "var", overwrite = TRUE, verbose = FALSE)
  
  # Move a non-existing object will raise an error
  expect_error(
    h5Move(to.file, "obs", "obs3", verbose = FALSE),
    "Cannot move a non-existing object"
  )
})

test_that("h5 backup", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  to.file <- file.path(dir, "tmp_test.h5")
  
  h5Backup(file, to.file, exclude = "X", verbose = FALSE)
  
  expect_false(h5Exists(to.file, "X"))
  x <- h5Read(file)
  x2 <- h5Read(to.file)
  
  for (i in names(x2)) {
    expect_identical(x2[[i]], x[[i]])
  }
})

test_that("h5 overwrite", {
  file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
  
  dir <- withr::local_tempdir()
  tmp.file <- file.path(dir, "tmp_test.h5")
  file.copy(file, tmp.file)
  
  obs <- h5Read(tmp.file, "obs")
  
  h5Overwrite(tmp.file, "layers", TRUE, verbose = FALSE)
  expect_false(h5Exists(tmp.file, "layers"))
  
  # You can still read other links.
  obs2 <- h5Read(tmp.file, "obs")
  expect_identical(obs, obs2)
})

