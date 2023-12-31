% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/hdf5-functions.R
\name{h5Overwrite}
\alias{h5Overwrite}
\title{Overwrite an existing HDF5 link}
\usage{
h5Overwrite(file, name, overwrite)
}
\arguments{
\item{file}{An existing HDF5 file}

\item{name}{Name of HDF5 link to be overwritten.}

\item{overwrite}{Whether or not to overwrite \code{name}.}
}
\value{
Path to \code{file} which is ready to be written.
}
\description{
Overwrite an existing HDF5 link
}
\details{
\itemize{
\item When \code{file} doesn't exist, will create it.
\item When the old link \code{name} doesn't exist, will simply return 
\code{file}. 
\item When \code{name} exists and \code{overwrite} is \code{TRUE}, will copy 
the rest of HDF5 links to an updated \code{file} with \code{\link{h5Backup}}. 
If \code{name} is "/", will create a new \code{file} and overwrite the old one.
\item When \code{name} exists and \code{overwrite} is \code{FALSE}, will 
raise an error.
}
}
\examples{
file <- system.file("extdata", "pbmc_small.h5ad", package = "hdf5r.Extra")
tmp.file <- tempfile(fileext = ".h5")
file.copy(file, tmp.file)

obs <- h5Read(tmp.file, "obs")

h5Overwrite(tmp.file, "layers", TRUE)
stopifnot(!h5Exists(tmp.file, "layers"))

# You can still read other links.
obs2 <- h5Read(tmp.file, "obs")
stopifnot(identical(obs, obs2))

}
