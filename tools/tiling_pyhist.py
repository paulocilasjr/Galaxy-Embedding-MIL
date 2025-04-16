import os
import zipfile
import tempfile
import subprocess
import shutil
from pathlib import Path
import logging
import concurrent.futures

# Configure logging
logging.basicConfig(
    filename="tile_processing.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

VALID_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.tif', '.tiff', '.svs', '.dat'}

def extract_zip(zip_file):
    """Extract a ZIP file to a temporary directory."""
    temp_dir = tempfile.mkdtemp(prefix="zip_extract_")
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
            file_list = [str(Path(temp_dir) / f) for f in zip_ref.namelist()]
        logging.info("ZIP file extracted to: %s", temp_dir)
        return temp_dir, file_list
    except zipfile.BadZipFile as exc:
        raise RuntimeError("Invalid ZIP file.") from exc

def pull_docker_image():
    """Pull the PyHIST Docker image."""
    try:
        subprocess.run(
            ["docker", "pull", "mmunozag/pyhist"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Pulled docker image: mmunozag/pyhist")
    except subprocess.CalledProcessError as e:
        logging.error("Failed to pull docker image: %s", e.stderr)
        raise RuntimeError(f"Failed to pull mmunozag/pyhist: {e.stderr}") from e

def run_pyhist_docker(image_path):
    """Run the PyHIST Docker container to process an image and generate tiles."""
    parent_dir = image_path.parent
    output_root = parent_dir / "output"
    output_root.mkdir(exist_ok=True)

    cmd = [
        "docker", "run", "--rm",
        "--platform", "linux/amd64",
        "-v", f"{parent_dir}:/pyhist/images",
        "mmunozag/pyhist",
        "--patch-size", "512",
        "--content-threshold", "0.4",
        "--output-downsample", "4",
        "--borders", "0000",
        "--corners", "1010",
        "--percentage-bc", "1",
        "--k-const", "1000",
        "--minimum_segmentsize", "1000",
        "--save-patches",
        "--save-tilecrossed-image",
        "--info", "verbose",
        "--output", "/pyhist/images/output",
        f"/pyhist/images/{image_path.name}"
    ]

    logging.info("Running docker command: %s", ' '.join(cmd))
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info("PyHIST docker executed successfully for %s", image_path)
    except subprocess.CalledProcessError as e:
        logging.error("PyHIST docker failed for %s: %s", image_path, e.stderr)
        raise RuntimeError(f"PyHIST docker processing failed: {e.stderr}") from e

    image_folder = output_root / image_path.stem
    expected_tile_folder = image_folder / f"{image_path.stem}_tiles"
    return expected_tile_folder

def process_files(input_path):
    """Process images in parallel and return a map of image names to tile directories."""
    input_path = Path(input_path).resolve()
    temp_dir = None
    image_paths = []

    # Collect all image paths
    if input_path.suffix.lower() in VALID_EXTENSIONS:
        image_paths.append(input_path)
    elif input_path.suffix.lower() == ".zip":
        temp_dir, file_list = extract_zip(input_path)
        for file_path in file_list:
            if Path(file_path).suffix.lower() in VALID_EXTENSIONS:
                image_paths.append(Path(file_path))
    else:
        raise ValueError(f"Unsupported input file type: {input_path.suffix}. Expected .zip or {VALID_EXTENSIONS}")

    # Process images in parallel
    image_tile_map = {}
    with concurrent.futures.ProcessPoolExecutor(max_workers=25) as executor:
        # Submit all image processing tasks
        future_to_image = {
            executor.submit(run_pyhist_docker, img_path): img_path
            for img_path in image_paths
        }
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_image):
            img_path = future_to_image[future]
            try:
                tile_dir = future.result()
                tile_files = list(tile_dir.glob("*.png"))
                if tile_files:
                    image_tile_map[img_path.stem] = tile_dir
                else:
                    logging.warning("No PNG tiles found in %s", tile_dir)
            except Exception as e:
                logging.error("Processing failed for %s: %s", img_path, e)
                # Continue processing other images; errors are logged

    return image_tile_map, temp_dir

def create_output_zip(image_tile_map, output_zip_path):
    """Create a ZIP file containing all generated tiles."""
    with zipfile.ZipFile(output_zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
        for image_name, tile_dir in image_tile_map.items():
            for file in tile_dir.glob("*.png"):
                arcname = f"{image_name}/{file.name}"
                zipf.write(file, arcname)
    logging.info("Output ZIP created: %s", output_zip_path)

def main(input_path, output_zip):
    """Main function to orchestrate image tiling and output creation."""
    pull_docker_image()
    image_tile_map, temp_dir = process_files(input_path)
    create_output_zip(image_tile_map, output_zip)
    if temp_dir:
        shutil.rmtree(temp_dir, ignore_errors=True)
    logging.info("Processing completed successfully.")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Tile images using PyHIST docker.")
    parser.add_argument("--input", required=True, help="Path to the input ZIP file or single image.")
    parser.add_argument("--output_zip", required=True, help="Path to the output ZIP file with tiles.")
    args = parser.parse_args()
    main(args.input, args.output_zip)
