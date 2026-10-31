//! On-demand thumbnail generation for HTTP image previews.

use std::path::Path;

use crate::common::catalog::find_supported_image_by_id;
use crate::common::error::Error;
use crate::common::loaders::ImageLoaderRegistry;
use crate::common::packer::downsample_bands;

/// Generate a thumbnail PNG for an image on-the-fly.
///
/// Loads the image, takes the first 3 bands (or fewer) as RGB, downscales to
/// fit within `max_dim`, and encodes as PNG.
pub async fn generate_thumbnail(
    data_dir: &Path,
    loaders: &ImageLoaderRegistry,
    image_id: &str,
    max_dim: u32,
) -> Result<Vec<u8>, Error> {
    let image_path = find_supported_image_by_id(data_dir, image_id, |path| loaders.supports(path))
        .ok_or_else(|| Error::ImageNotFound(image_id.to_string()))?;

    let loader = loaders
        .find_loader(&image_path)
        .ok_or_else(|| Error::UnsupportedFormat(image_id.to_string()))?;

    let bands = loader.load_bands(&image_path).await?;
    let (tw, th) = thumbnail_size(bands.width, bands.height, max_dim);
    let downsampled = if tw != bands.width || th != bands.height {
        downsample_bands(&bands, tw, th)
    } else {
        bands
    };

    let num_bands = downsampled.bands.len();
    let pixel_count = (tw * th) as usize;
    let mut rgba = vec![255u8; pixel_count * 4];
    for i in 0..pixel_count {
        let r = (downsampled.bands[0][i] * 255.0).clamp(0.0, 255.0) as u8;
        let g = if num_bands >= 3 {
            (downsampled.bands[1][i] * 255.0).clamp(0.0, 255.0) as u8
        } else {
            r
        };
        let b = if num_bands >= 3 {
            (downsampled.bands[2][i] * 255.0).clamp(0.0, 255.0) as u8
        } else {
            r
        };
        rgba[i * 4] = r;
        rgba[i * 4 + 1] = g;
        rgba[i * 4 + 2] = b;
    }

    let img = image::RgbaImage::from_raw(tw, th, rgba)
        .ok_or_else(|| Error::Internal("Failed to create image buffer".to_string()))?;
    let mut buf = std::io::Cursor::new(Vec::new());
    img.write_to(&mut buf, image::ImageFormat::Png)
        .map_err(|e| Error::Internal(format!("PNG encode failed: {e}")))?;

    Ok(buf.into_inner())
}

fn thumbnail_size(width: u32, height: u32, max_dim: u32) -> (u32, u32) {
    let max = width.max(height);
    if max <= max_dim {
        return (width, height);
    }
    let scale = max_dim as f64 / max as f64;
    (
        (width as f64 * scale).round().max(1.0) as u32,
        (height as f64 * scale).round().max(1.0) as u32,
    )
}
