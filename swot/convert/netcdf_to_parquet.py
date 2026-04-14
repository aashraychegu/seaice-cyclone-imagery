import xarray as xr
import numpy as np
from osgeo import gdal, osr
import rioxarray

def swot_ssh_to_geotiff(netcdf_path, output_path, variable='ssh_karin', 
                        resolution=0.001, crs='EPSG:4326'):
    """
    Convert SWOT SSH data to GeoTIFF.
    
    Parameters:
    -----------
    netcdf_path : str
        Path to SWOT L2 LR SSH NetCDF file
    output_path : str
        Path for output GeoTIFF
    variable : str
        Variable to extract (default: 'ssh_karin')
    resolution : float
        Grid resolution in degrees (default: 0.001 = ~100m)
    crs : str
        Coordinate reference system
    """
    
    # Open the NetCDF file
    ds = xr.open_dataset(netcdf_path)
    
    # Extract data with proper scaling
    ssh_data = ds[variable].values
    lat_data = ds['latitude'].values
    lon_data = ds['longitude'].values
    
    # Apply scale factor and handle nodata
    scale = ds[variable].attrs.get('scale', 0.0001)
    nodata = ds[variable].attrs.get('nodata_value', 2147483647)
    
    # Convert to float and apply scaling
    ssh_data = ssh_data.astype(np.float32)
    ssh_data[ssh_data == nodata] = np.nan
    ssh_data = ssh_data * scale
    
    # Latitude also needs scaling
    lat_scale = ds['latitude'].attrs.get('scale', 0.000001)
    lon_scale = ds['longitude'].attrs.get('scale', 0.000001)
    lat_nodata = ds['latitude'].attrs.get('nodata_value', 2147483647)
    lon_nodata = ds['longitude'].attrs.get('nodata_value', 2147483647)
    
    lat_data = lat_data.astype(np.float64)
    lon_data = lon_data.astype(np.float64)
    lat_data[lat_data == lat_nodata] = np.nan
    lon_data[lon_data == lon_nodata] = np.nan
    lat_data = lat_data * lat_scale
    lon_data = lon_data * lon_scale
    
    print(f"Data shape: {ssh_data.shape}")
    print(f"SSH range: {np.nanmin(ssh_data):.2f} to {np.nanmax(ssh_data):.2f} m")
    print(f"Lat range: {np.nanmin(lat_data):.2f} to {np.nanmax(lat_data):.2f}")
    print(f"Lon range: {np.nanmin(lon_data):.2f} to {np.nanmax(lon_data):.2f}")
    
    # Create regular grid for interpolation
    lon_min, lon_max = np.nanmin(lon_data), np.nanmax(lon_data)
    lat_min, lat_max = np.nanmin(lat_data), np.nanmax(lat_data)
    
    grid_lon = np.arange(lon_min, lon_max + resolution, resolution)
    grid_lat = np.arange(lat_max, lat_min - resolution, -resolution)
    
    print(f"\nCreating grid: {len(grid_lat)} x {len(grid_lon)}")
    
    # Interpolate to regular grid
    from scipy.interpolate import griddata
    
    # Flatten arrays and remove NaNs
    points = np.column_stack((lon_data.flatten(), lat_data.flatten()))
    values = ssh_data.flatten()
    
    valid_mask = ~(np.isnan(values) | np.isnan(points[:, 0]) | np.isnan(points[:, 1]))
    points = points[valid_mask]
    values = values[valid_mask]
    
    print(f"Valid points for interpolation: {len(values)}")
    
    # Create grid
    grid_lon_mesh, grid_lat_mesh = np.meshgrid(grid_lon, grid_lat)
    
    print("Interpolating to regular grid (this may take a moment)...")
    grid_ssh = griddata(points, values, (grid_lon_mesh, grid_lat_mesh), 
                        method='linear', fill_value=np.nan)
    
    # Create GeoTIFF
    print("Writing GeoTIFF...")
    driver = gdal.GetDriverByName('GTiff')
    rows, cols = grid_ssh.shape
    
    dataset = driver.Create(
        output_path, 
        cols, 
        rows, 
        1, 
        gdal.GDT_Float32,
        options=['COMPRESS=LZW', 'PREDICTOR=3', 'TILED=YES']
    )
    
    # Set geotransform (top-left corner, pixel size)
    geotransform = (lon_min, resolution, 0, lat_max, 0, -resolution)
    dataset.SetGeoTransform(geotransform)
    
    # Set projection
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(crs.split(':')[1]))
    dataset.SetProjection(srs.ExportToWkt())
    
    # Write data
    band = dataset.GetRasterBand(1)
    band.WriteArray(grid_ssh)
    band.SetNoDataValue(np.nan)
    
    # Set metadata
    band.SetDescription(f"SWOT {variable}")
    dataset.SetMetadataItem('VARIABLE', variable)
    dataset.SetMetadataItem('UNITS', 'm')
    dataset.SetMetadataItem('LONG_NAME', ds[variable].attrs.get('long_name', ''))
    dataset.SetMetadataItem('SOURCE', f"SWOT Cycle {ds.attrs.get('cycle_number')}, Pass {ds.attrs.get('pass_number')}")
    
    # Flush and close
    band.FlushCache()
    dataset = None
    
    print(f"\n✓ Successfully created: {output_path}")
    print(f"  Dimensions: {rows} x {cols}")
    print(f"  Resolution: {resolution}°")
    
    ds.close()


def swot_ssh_direct_export(netcdf_path, output_path, variable='ssh_karin'):
    """
    Alternative method: Direct export without interpolation (keeps swath geometry).
    This creates a GeoTIFF but it won't display properly in most GIS software
    without proper warping, as it has 2D lat/lon coordinates.
    """
    
    ds = xr.open_dataset(netcdf_path)
    
    # Extract and scale data
    data = ds[variable]
    scale = data.attrs.get('scale', 0.0001)
    nodata = data.attrs.get('nodata_value', 2147483647)
    
    # Convert to proper values
    data_values = data.values.astype(np.float32)
    data_values[data_values == nodata] = np.nan
    data_values = data_values * scale
    
    # Create xarray DataArray with metadata
    data_array = xr.DataArray(
        data_values,
        dims=['num_lines', 'num_pixels'],
        attrs={
            'long_name': data.attrs.get('long_name'),
            'units': 'm'
        }
    )
    
    # Write CRS
    data_array.rio.write_crs('EPSG:4326', inplace=True)
    data_array.rio.write_nodata(np.nan, inplace=True)
    
    # Export to GeoTIFF
    data_array.rio.to_raster(output_path, driver='GTiff', compress='LZW')
    
    print(f"Exported swath data to: {output_path}")
    print("Note: This file needs georeferencing with lat/lon arrays for proper GIS use")
    
    ds.close()


# Example usage
if __name__ == "__main__":
    
    input_file = "SWOT_L2_LR_SSH_020_506_20240907T165059_20240907T174234_PIC0_01.nc"
    
    # Method 1: Gridded output (recommended for GIS)
    print("=" * 70)
    print("METHOD 1: Creating gridded GeoTIFF (interpolated to regular grid)")
    print("=" * 70)
    swot_ssh_to_geotiff(
        netcdf_path=input_file,
        output_path='swot_ssh_karin_gridded.tif',
        variable='ssh_karin',
        resolution=0.001,  # ~100m resolution
        crs='EPSG:4326'
    )
    
    # Also export SSH anomaly
    print("\n" + "=" * 70)
    print("Creating SSHA (Sea Surface Height Anomaly)")
    print("=" * 70)
    swot_ssh_to_geotiff(
        netcdf_path=input_file,
        output_path='swot_ssha_karin_gridded.tif',
        variable='ssha_karin',
        resolution=0.001,
        crs='EPSG:4326'
    )