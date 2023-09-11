
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# S3 generics ##################################################################
#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

## HDF5 helpers ================================================================

#' Check existence of an HDF5 link
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file
#' @param name Name of HDF5 link to be checked.
#' @param ... Arguments passed to \code{H5File$exists()}
#' 
#' @return If any parent directory of \code{name} doesn't exist, will simply 
#' return \code{FALSE}
#' 
#' @rdname h5Exists
#' @export h5Exists
h5Exists <- function(x, name, ...) {
  UseMethod(generic = "h5Exists", object = x)
}

#' Get information of an HDF5 dataset
#' 
#' Functions to get the information from an HDF5 dataset.
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}}, 
#' \code{\link[hdf5r]{H5D}} or a path name of HDF5 file.
#' @param ... Arguments passed to other methods.
#' 
#' @seealso \code{\link[hdf5r]{H5D-class}}
#' 
#' @name H5-dataset-info
NULL

#' @rdname H5-dataset-info
#' @export h5Dims
h5Dims <- function(x, ...) {
  UseMethod(generic = "h5Dims", object = x)
}

#' @rdname H5-dataset-info
#' @export h5MaxDims
h5MaxDims <- function(x, ...) {
  UseMethod(generic = "h5MaxDims", object = x)
}

#' List the contents of an HDF5 group
#' 
#' Function to list the contents of an HDF5 group.
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file.
#' @param ... Additional parameters passed to \code{$ls()}
#' 
#' @return 
#' If \code{simplify}, will return a character vector specifying names of H5 
#' links, otherwise will return a \code{data.frame} to show details.
#' 
#' @rdname h5List
#' @export h5List
h5List <- function(x, ...) {
  UseMethod(generic = "h5List", object = x)
}

#' Create a new HDF5 file
#' 
#' A wrapper for \code{\link[hdf5r]{H5File}}\code{$new()}. If \code{file} 
#' exists, will only raise a warning.
#' 
#' @param x Name of the new HDF5 file.
#' @param ... Arguments passed to \code{H5File$new()}
#' 
#' @rdname h5CreateFile
#' @export h5CreateFile
h5CreateFile <- function(x, ...) {
  UseMethod(generic = "h5CreateFile", object = x)
}

#' Create new HDF5 group
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file.
#' @param name Name of the new HDF5 group. Can be recursive, such as "g/sub_g".
#' @param show.warnings When the group \code{name} already exists, whether or 
#' not to show warning messages.
#' @param ... Arguments passed to \code{H5Group$create_group()}.
#' 
#' @seealso \code{\link[hdf5r]{H5Group}}
#' 
#' @rdname h5CreateGroup
#' @export h5CreateGroup
h5CreateGroup <- function(x, name, ...) {
  UseMethod(generic = "h5CreateGroup", object = x)
}

#' Create a new empty HDF5 dataset
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file.
#' @param name Name of the new HDF5 dataset.
#' @param ... Arguments passed to \code{H5Group$create_dataset()}.
#' 
#' @seealso \code{\link[hdf5r]{H5File}} and \code{\link[hdf5r]{H5Group}} for the 
#' \code{$create_dataset()} methods.
#' 
#' @rdname h5CreateDataset
#' @export h5CreateDataset
h5CreateDataset <- function(x, name, ...) {
  UseMethod(generic = "h5CreateDataset", object = x)
}

#' Open an HDF5 file, file-handler or group object
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file.
#' @param name Name of the opened HDF5 link.
#' @param ... Arguments passed to \code{H5Group$open()}.
#' 
#' @return An opened \code{\link[hdf5r]{H5Group}} or \code{\link[hdf5r]{H5D}}.
#' 
#' @rdname h5Open
#' @export h5Open
h5Open <- function(x, name, ...) {
  UseMethod(generic = "h5Open", object = x)
}

#' Delete an HDF5 link
#' 
#' @param x An existing HDF5 file
#' @param name Name of HDF5 link to be deleted. If \code{name} doesn't exist, 
#' nothing will be done.
#' @param ... Arguments passed to \code{H5Group$link_delete()}
#' 
#' @rdname h5Delete
#' @export h5Delete
h5Delete <- function(x, name, ...) {
  UseMethod(generic = "h5Delete", object = x)
}

#' Manipulate HDF5 attributes
#' 
#' Functions to get, set or delete HDF5 attributes for an existing link.
#' 
#' @name H5-attributs
NULL

#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}}, 
#' \code{\link[hdf5r]{H5D}} or a path name of HDF5 file.
#' @param which Name of the HDF5 attribute
#' @param ... Arguments passed to other methods.
#' 
#' @rdname H5-attributs
#' @export h5Attr
h5Attr <- function(x, which, ...) {
  UseMethod(generic = "h5Attr", object = x)
}

#' @rdname H5-attributs
#' @export h5Attributes
h5Attributes <- function(x, ...) {
  UseMethod(generic = "h5Attributes", object = x)
}

#' @rdname H5-attributs
#' @export h5AttrNames
h5AttrNames <- function(x, ...) {
  UseMethod(generic = "h5AttrNames", object = x)
}

#' @rdname H5-attributs
#' @export h5DeleteAttr
h5DeleteAttr <- function(x, which, ...) {
  UseMethod(generic = "h5DeleteAttr", object = x)
}

#' @param robj An R object to be written as HDF5 attribute
#' 
#' @rdname H5-attributs
#' @export h5WriteAttr
h5WriteAttr <- function(x, which, robj, ...) {
  UseMethod(generic = "h5WriteAttr", object = x)
}

#' Write a scalar into HDF5 file
#' 
#' Low-level helper function to write scalar R data into HDF5 dataset. Data will 
#' be written into scalar space instead of array space.
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file.
#' @param name Name of an HDF5 link.
#' @param robj A scalar object.
#' @param ... Arguments passed to \code{H5File$create_dataset()}. See 
#' \code{link[hdf5r:H5File-class]{H5File}}.
#' 
#' @note
#' If you want to write \code{robj} into array space, you should use 
#' \code{\link{h5WriteDataset}}.
#' 
#' @rdname h5WriteScalar
#' @export h5WriteScalar
h5WriteScalar <- function(x, name, robj, ...) {
  UseMethod(generic = "h5WriteScalar", object = x)
}

#' Write array-like data into an existing H5 dataset
#' 
#' Low-level helper function to write atomic R data into an existing H5 dataset. 
#' All data written will be treated as array for HDF5.
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}}, 
#' \code{\link[hdf5r]{H5D}} or a path name of HDF5 file.
#' @param robj An R array.
#' @param ... Arguments passed to \code{H5D$write()}.
#' 
#' @note
#' If you want to write \code{robj} into scalar space, you should use 
#' \code{\link{h5WriteScalar}}.
#' 
#' @rdname h5WriteDataset
#' @export h5WriteDataset
h5WriteDataset <- function(x, robj, ...) {
  UseMethod(generic = "h5WriteDataset", object = x)
}

#' Read data from an existing H5 dataset
#' 
#' Low-level helper function to read atomic R data from an existing H5 dataset.
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}}, 
#' \code{\link[hdf5r]{H5D}} or a path name of HDF5 file.
#' @param ... Arguments passed to \code{H5D$read()}.
#' 
#' @return 
#' An array-like object with the data read.
#' 
#' @rdname h5ReadDataset
#' @export h5ReadDataset
h5ReadDataset <- function(x, ...) {
  UseMethod(generic = "h5ReadDataset", object = x)
}

#' Read data from an existing HDF5 link
#' 
#' Function to read data from an existing HDF5 group.
#' 
#' @param x An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}} or a 
#' path name of HDF5 file.
#' @param ... Arguments passed to \code{\link{h5ReadDataset}}.
#' 
#' @rdname h5Read
#' @export h5Read
h5Read <- function(x, name = NULL, ...) {
  UseMethod(generic = "h5Read", object = x)
}

#' Write an R object to HDF5 file
#' 
#' Methods to write an R object to an HDF5 file.
#' 
#' @param x An R object to be written
#' @param file An existing HDF5 file
#' @param name Name of the HDF5 link to be written into
#' @param ... Arguments passed to other methods.
#' 
#' @rdname h5Write
#' @export h5Write
h5Write <- function(x, file, name, ...) {
  UseMethod(generic = "h5Write", object = x)
}

#' Prepare an R object to be written into HDF5 file
#' 
#' Methods to transform a complex R object (for example, S4 object) into 
#' combination of base R objects, such as \code{vector}, \code{array}, 
#' \code{data.frame} or \code{list}, so that it can be written into HDF5 file.
#' 
#' @param x The R object to be transformed
#' @param ... Arguments to be passed to other methods
#' 
#' @details 
#' In this package, \code{h5Prep} will return \code{x} itself by default. 
#' Extended methods can be easily added for specific S4 class.
#' 
#' @rdname h5Prep
#' @export h5Prep
h5Prep <- function(x, ...) {
  UseMethod(generic = "h5Prep", object = x)
}
