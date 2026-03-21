import xarray

dataset = xarray.open_dataset("intermediates/sea_ice_concentration_2024.nc")
print(dataset.variables)