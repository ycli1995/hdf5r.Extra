# hdf5r.Extra 0.1.0
## Add
* Add `tests`
* Add `add.dimnames` parameter in `h5Write` method for sparse matrix.

## Removal
* Remove `toS4.func` for `h5Read`.
* Remove `dimnames` parameter in `h5Write` method for sparse matrix.

## Change
* `h5Open` cannot open `"/"` for an H5Group.

# hdf5r.Extra 0.0.6
## Add
* Use `verboseMsg()` to handle progress info.

# hdf5r.Extra 0.0.5
## Bug fixes
* Fixed bugs caused by `is.null` to be compatible with `hdf5r` 1.3.9.

# hdf5r.Extra 0.0.4
## Bug fixes
* Fixed bugs caused by `h5Open` when open the root of a HDF5 file.
* Fixed bugs caused by `.h5read_dataframe`.

# hdf5r.Extra 0.0.3
## Bug fixes
* Fixed bugs caused by `h5Write` when transform an S4 object to list.

## Improvement
* Clarify usage of `toS4.func` for `h5Read`.

# hdf5r.Extra 0.0.2
## Bug fixes
* Fixed bugs caused by `H5Backup` in old R release platform.
* Fixed bugs caused by not using `tempfile` in examples.

## Improvement
* Add more formatted messages for verbose.

# hdf5r.Extra 0.0.1

* Initial release.
