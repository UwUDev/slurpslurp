use discord_client_structs::structs::message::attachment::Attachment;
use discord_client_structs::structs::message::embed::Embed;
use log::{error, info, warn};
use mime_guess;
use rquest::Client;
use rquest_util::{Emulation, EmulationOS, EmulationOption};
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::Mutex as AsyncMutex;
use tree_magic_mini;

use sanitise_file_name::sanitise;

fn sanitize_filename(filename: &str) -> String {
    let clean_filename = filename.split('?').next().unwrap_or(filename);

    let decoded = urlencoding::decode(clean_filename)
        .map(|s| s.into_owned())
        .unwrap_or_else(|_| clean_filename.to_string());

    let sanitized = sanitise(&decoded);
    const MAX_LENGTH: usize = 200;

    if sanitized.len() <= MAX_LENGTH {
        return sanitized;
    }

    if let Some(dot_pos) = sanitized.rfind('.') {
        let name = &sanitized[..dot_pos];
        let ext = &sanitized[dot_pos..];

        if ext.len() >= MAX_LENGTH {
            // Extension too long, truncate the whole name
            return sanitized.chars().take(MAX_LENGTH).collect();
        }

        let available_for_name = MAX_LENGTH - ext.len();
        let truncated_name = name.chars().take(available_for_name).collect::<String>();

        format!("{}{}", truncated_name, ext)
    } else {
        sanitized.chars().take(MAX_LENGTH).collect()
    }
}

async fn detect_mime_type(attachment: &Attachment, url: &str) -> Result<String, Box<dyn Error>> {
    // Use content type from attachment if available
    if let Some(content_type) = &attachment.content_type {
        let cleaned_type = content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim();
        if !cleaned_type.is_empty() && cleaned_type != "application/octet-stream" {
            return Ok(cleaned_type.to_string());
        }
    }

    // Download the file to a temporary location
    let temp_file = NamedTempFile::new()?;
    let temp_path = temp_file.path();

    if let Ok(_) = download_url(url, temp_path.to_str().unwrap()).await {
        // Detect MIME type from file content
        if let Some(mime_from_content) = tree_magic_mini::from_filepath(temp_path) {
            if mime_from_content != "application/octet-stream" {
                return Ok(mime_from_content.to_string());
            }
        }
    }

    // Fallback on filename extension
    let filename = &attachment.filename;
    let mime_from_extension = mime_guess::from_path(filename).first_or_octet_stream();

    if mime_from_extension != mime::APPLICATION_OCTET_STREAM {
        Ok(mime_from_extension.to_string())
    } else {
        // Unable to determine MIME type, return default "binary" type
        Ok("application/octet-stream".to_string())
    }
}

pub async fn download_attachment(attachments: Vec<Attachment>) -> Result<(), Box<dyn Error>> {
    for attachment in attachments {
        let url = &attachment.url;
        let original_filename = attachment.filename.clone();

        let mime_type = detect_mime_type(&attachment, url)
            .await
            .unwrap_or_else(|_| "application/octet-stream".to_string());

        let safe_filename = sanitize_filename(&original_filename);
        std::fs::create_dir_all(format!("downloads/{}", mime_type))?;

        let final_filename = format!(
            "downloads/{}/{}_{}",
            mime_type, attachment.id, safe_filename
        );

        if Path::new(&final_filename).exists() {
            warn!("File already exists: {}", final_filename);
            continue;
        }

        if let Err(e) = download_url(url, &final_filename).await {
            error!("Failed to download {}: {}", final_filename, e);
        }
    }

    Ok(())
}

pub async fn download_embeds(embeds: Vec<Embed>, message_id: u64) -> Result<(), Box<dyn Error>> {
    let mut urls: Vec<(String, &str)> = Vec::new();

    for embed in embeds {
        if let Some(image) = &embed.image {
            let url = match image.clone().proxy_url {
                Some(proxy_url) => proxy_url,
                None => image.url.clone(),
            };
            urls.push((url, "image"));
        }

        if let Some(thumbnail) = &embed.thumbnail {
            let url = match thumbnail.clone().proxy_url {
                Some(proxy_url) => proxy_url,
                None => thumbnail.url.clone(),
            };
            urls.push((url, "image"));
        }

        if let Some(video) = &embed.video {
            let url = match video.clone().proxy_url {
                Some(proxy_url) => proxy_url,
                None => video.url.clone(),
            };
            urls.push((url, "video"));
        }
    }

    for (url, media_type) in urls {
        let extension = extract_extension_from_url(&url, media_type);

        let folder_path = format!("downloads/{}/{}", media_type, extension);
        std::fs::create_dir_all(&folder_path)?;

        let file_name = format!(
            "{}/{}_{}",
            folder_path,
            message_id,
            sanitize_filename(url.split('/').last().unwrap_or("unknown"))
        );

        if Path::new(&file_name).exists() {
            warn!("File already exists: {}", file_name);
            continue;
        }

        if let Err(e) = download_url(&url, &file_name).await {
            error!("Failed to download {}: {}", file_name, e);
        }
    }

    Ok(())
}

fn extract_extension_from_url(url: &str, media_type: &str) -> String {
    let clean_url = url.split(['?', '#']).next().unwrap_or(url);

    if let Some(last_dot) = clean_url.rfind('.') {
        let potential_ext = &clean_url[last_dot + 1..];

        if potential_ext.len() <= 5 && potential_ext.chars().all(|c| c.is_alphanumeric()) {
            let ext = potential_ext.to_lowercase().replace("jpg", "jpeg");

            if is_valid_extension_for_media_type(&ext, media_type) {
                return ext;
            }
        }
    }

    match media_type {
        "image" => "jpeg",
        "video" => "mp4",
        _ => "bin",
    }
    .to_string()
}

fn is_valid_extension_for_media_type(extension: &str, media_type: &str) -> bool {
    match media_type {
        "image" => matches!(
            extension,
            "jpeg" | "jpg" | "png" | "gif" | "webp" | "bmp" | "svg"
        ),
        "video" => matches!(
            extension,
            "mp4" | "webm" | "avi" | "mov" | "mkv" | "flv" | "wmv"
        ),
        _ => true,
    }
}

lazy_static::lazy_static! {
    static ref CACHE: Arc<AsyncMutex<Vec<String>>> = Arc::new(AsyncMutex::new(Vec::with_capacity(5)));
}

pub async fn download_url(url: &str, file_name: &str) -> Result<(), Box<dyn Error>> {
    let mut cache = CACHE.lock().await;
    if cache.contains(&url.to_string()) {
        return Ok(());
    }
    if cache.len() >= 5 {
        cache.remove(0);
    }
    cache.push(url.to_string());
    drop(cache);

    let emu = EmulationOption::builder()
        .emulation(Emulation::Chrome136)
        .emulation_os(EmulationOS::Windows)
        .build();

    let client = Client::builder()
        .emulation(emu)
        .gzip(true)
        .deflate(true)
        .brotli(true)
        .zstd(true)
        .build()?;

    let response = client.get(url).send().await?;

    if response.status().is_success() {
        let bytes = response.bytes().await?;
        std::fs::write(file_name, bytes)?;
        info!("Downloaded: {}", file_name);
    } else {
        error!("Failed to download {}: {}", file_name, response.status());
    }

    Ok(())
}
