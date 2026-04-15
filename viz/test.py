import gradio as gr
import rasterio
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Constant path to the GeoTIFF file
GEOTIFF_PATH = "./intermediates/tiffs/00edeb1e-a61d-43ba-a8fa-a5d9f929d63c/s1a-ew-grd-hh-20141010t074510-20141010t074610-002764-0031bd-001-cog.tiff"  # Replace with your actual file path

def load_and_display_geotiff():
    """Load a GeoTIFF file from a constant path and display it over a world map."""
    try:
        # Open the GeoTIFF file
        with rasterio.open(GEOTIFF_PATH) as src:
            # Read the first band
            band = src.read(1)
            
            # Get the transform and bounds
            transform = src.transform
            bounds = src.bounds
            
            # Get the CRS of the GeoTIFF
            src_crs = src.crs
            
            # Create a figure with a PlateCarree projection
            fig = plt.figure(figsize=(12, 8))
            ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
            
            # Add world map features
            ax.add_feature(cfeature.LAND, color='lightgray')
            ax.add_feature(cfeature.OCEAN, color='lightblue')
            ax.add_feature(cfeature.COASTLINE, linewidth=0.5)
            ax.add_feature(cfeature.BORDERS, linestyle=':', linewidth=0.5)
            ax.add_feature(cfeature.LAKES, color='lightblue', alpha=0.5)
            ax.add_feature(cfeature.RIVERS, color='blue', alpha=0.3)
            
            # Handle different CRS scenarios
            if src_crs is None or src_crs.is_geographic:
                # If the data is already in geographic coordinates (lat/lon)
                display_transform = ccrs.PlateCarree()
            else:
                # If the data is in projected coordinates, we need to transform it
                # For now, we'll try to use the source CRS directly
                try:
                    display_transform = ccrs.epsg(src_crs.to_epsg())
                except:
                    # Fallback to PlateCarree if we can't determine the projection
                    display_transform = ccrs.PlateCarree()
            
            # Display the GeoTIFF data with proper transformation
            img = ax.imshow(band, cmap='viridis', 
                          extent=(bounds.left, bounds.right, bounds.bottom, bounds.top),
                          transform=display_transform,
                          alpha=0.7,
                          origin='upper')
            
            # Add a colorbar
            cbar = plt.colorbar(img, ax=ax, label='Value', orientation='horizontal', pad=0.05, fraction=0.02)
            
            # Set title and gridlines
            ax.set_title(f'GeoTIFF Overlay on World Map\nCRS: {src_crs if src_crs else "Unknown"}')
            ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5)
            
            # Set appropriate extent based on the data bounds
            ax.set_extent([bounds.left, bounds.right, bounds.bottom, bounds.top], crs=display_transform)
            
            return fig
    except Exception as e:
        # Create an error figure instead of returning a string
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.text(0.5, 0.5, f"Error: {str(e)}", ha='center', va='center', fontsize=12)
        ax.set_title('Error Loading GeoTIFF')
        ax.axis('off')
        return fig

# Create the Gradio interface
with gr.Blocks() as app:
    gr.Markdown("# GeoTIFF World Map Overlay")
    gr.Markdown(f"Displaying GeoTIFF from: `{GEOTIFF_PATH}`")
    
    with gr.Row():
        output_plot = gr.Plot(label="GeoTIFF Overlay on World Map")
    
    with gr.Row():
        display_button = gr.Button("Display GeoTIFF Overlay")
    
    # Connect the button to the function
    display_button.click(
        fn=load_and_display_geotiff,
        inputs=None,
        outputs=output_plot
    )

# Launch the app
if __name__ == "__main__":
    app.launch(share=True)
