% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/AllGenerics.R, R/hdf5-methods.R
\name{H5-dataset-info}
\alias{H5-dataset-info}
\alias{h5Dims}
\alias{h5MaxDims}
\alias{h5Dims.H5D}
\alias{h5Dims.H5Group}
\alias{h5Dims.H5File}
\alias{h5Dims.character}
\alias{h5MaxDims.H5D}
\alias{h5MaxDims.H5Group}
\alias{h5MaxDims.H5File}
\alias{h5MaxDims.character}
\title{Get information of an HDF5 dataset}
\usage{
h5Dims(x, ...)

h5MaxDims(x, ...)

\method{h5Dims}{H5D}(x, ...)

\method{h5Dims}{H5Group}(x, name, ...)

\method{h5Dims}{H5File}(x, name, ...)

\method{h5Dims}{character}(x, name, ...)

\method{h5MaxDims}{H5D}(x, ...)

\method{h5MaxDims}{H5D}(x, ...)

\method{h5MaxDims}{H5Group}(x, name, ...)

\method{h5MaxDims}{H5File}(x, name, ...)

\method{h5MaxDims}{character}(x, name, ...)
}
\arguments{
\item{x}{An \code{\link[hdf5r]{H5File}}, \code{\link[hdf5r]{H5Group}}, 
\code{\link[hdf5r]{H5D}} or a path name of HDF5 file.}

\item{...}{Arguments passed to other methods.}

\item{name}{A link in \code{file}. Must represent an H5D. Used when \code{x} 
is an \code{H5Group}, \code{H5File} or an HDF5 file.}
}
\value{
\code{h5Dims} returns the dimension of the HDF5 dataset.

\code{h5MaxDims} returns the maximal dimension of the HDF5 dataset.
}
\description{
Functions to get the information from an HDF5 dataset.
}
\examples{
file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
h5obj <- h5Open(file, "X", mode = "r")

h5Dims(file, "X")
h5Dims(h5obj)

h5MaxDims(file, "X")
h5MaxDims(h5obj)

}
\seealso{
\code{\link[hdf5r]{H5D-class}}
}
