import xarray

dataset = xarray.open_dataset("./intermediates/era5_mslp/2015_mslp.nc")
print(dataset.variables)