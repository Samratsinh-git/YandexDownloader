import argparse
import requests
import urllib.parse
import os
import concurrent.futures
from tqdm import tqdm

class YandexDiskDownloader:
    def __init__(self, link, download_location, num_threads=8):
        """
        Initializes the downloader.

        Args:
            link (str): The public link to the Yandex Disk file.
            download_location (str): The local directory to save the file.
            num_threads (int): The number of parallel download threads.
        """
        self.link = link
        self.download_location = download_location
        self.num_threads = num_threads
        self.session = requests.Session() # Use a session for connection pooling

    def _get_download_info(self):
        """Gets the direct download URL, filename, and total size."""
        api_url = f"https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key={self.link}"
        response = self.session.get(api_url)
        response.raise_for_status() # Raise an exception for HTTP errors
        
        download_url = response.json()["href"]
        
        # Get filename from URL parameters
        query_params = urllib.parse.parse_qs(urllib.parse.urlparse(download_url).query)
        file_name = query_params.get("filename", ["unknown_file"])[0]

        # Get file size and check for range support
        head_response = self.session.head(download_url, allow_redirects=True)
        head_response.raise_for_status()
        
        file_size = int(head_response.headers.get('content-length', 0))
        accept_ranges = head_response.headers.get('accept-ranges', '')

        return download_url, file_name, file_size, accept_ranges == 'bytes'

    def _download_chunk(self, args):
        """Downloads a single chunk of the file."""
        url, start, end, part_path, pbar = args
        headers = {'Range': f'bytes={start}-{end}'}
        
        try:
            with self.session.get(url, headers=headers, stream=True, timeout=30) as response:
                response.raise_for_status()
                with open(part_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024*1024): # Larger chunk size for writing
                        if chunk:
                            file.write(chunk)
                            pbar.update(len(chunk))
        except requests.RequestException as e:
            print(f"\nError downloading part {part_path}: {e}")
            # Mark this part as failed by creating an empty file or another indicator
            if os.path.exists(part_path):
                os.remove(part_path)
            return False
        return True

    def _merge_files(self, final_path, num_parts):
        """Merges the downloaded parts into a single file."""
        print(f"\nMerging {num_parts} parts into {os.path.basename(final_path)}...")
        try:
            with open(final_path, "wb") as final_file:
                for i in range(num_parts):
                    part_path = f"{final_path}.part{i}"
                    if not os.path.exists(part_path):
                        raise FileNotFoundError(f"Part file {part_path} is missing. Download may have failed.")
                    with open(part_path, "rb") as part_file:
                        final_file.write(part_file.read())
        finally:
            # Clean up part files
            print("Cleaning up temporary files...")
            for i in range(num_parts):
                part_path = f"{final_path}.part{i}"
                if os.path.exists(part_path):
                    os.remove(part_path)

    def download(self):
        """Main download method to orchestrate the download process."""
        try:
            url, file_name, file_size, supports_ranges = self._get_download_info()
            save_path = os.path.join(self.download_location, file_name)

            # Fallback to simple download if size is unknown or ranges are not supported
            if not file_size or not supports_ranges:
                print("File size unknown or server does not support parallel downloads. Falling back to single-threaded download.")
                self._simple_download(url, save_path, file_size)
                return

            print(f"Downloading {file_name} ({file_size / 1024 / 1024:.2f} MB) using {self.num_threads} threads.")
            
            chunk_size = file_size // self.num_threads
            tasks = []
            
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name) as pbar:
                with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                    for i in range(self.num_threads):
                        start_byte = i * chunk_size
                        end_byte = start_byte + chunk_size - 1
                        if i == self.num_threads - 1:
                            end_byte = file_size - 1 # Ensure the last chunk covers the remainder
                        
                        part_path = f"{save_path}.part{i}"
                        tasks.append(executor.submit(self._download_chunk, (url, start_byte, end_byte, part_path, pbar)))
                    
                    # Wait for all downloads to complete
                    results = [future.result() for future in concurrent.futures.as_completed(tasks)]

            if not all(results):
                 raise ConnectionError("One or more parts failed to download. Aborting merge.")

            self._merge_files(save_path, self.num_threads)
            print("Download complete.")

        except requests.RequestException as e:
            print(f"An error occurred: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def _simple_download(self, url, save_path, file_size):
        """A simple, single-threaded downloader for fallback."""
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=os.path.basename(save_path)) as pbar:
            with self.session.get(url, stream=True) as response:
                response.raise_for_status()
                with open(save_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=1024*1024):
                        if chunk:
                            file.write(chunk)
                            pbar.update(len(chunk))
        print("\nDownload complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Yandex Disk Downloader with parallel connections for improved speed.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-l', '--link', type=str, help='Public link for the Yandex Disk URL', required=True)
    # This is the corrected line:
    parser.add_argument('-d', '--download_location', type=str, help='Download location on your PC', required=True)
    parser.add_argument('-t', '--threads', type=int, default=8, help='Number of parallel download threads (default: 8)')
    args = parser.parse_args()

    # Ensure the download directory exists
    os.makedirs(args.download_location, exist_ok=True)

    downloader = YandexDiskDownloader(args.link, args.download_location, args.threads)
    downloader.download()