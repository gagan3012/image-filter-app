import io
import json
import os
import re
from typing import List, Dict, Any
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import base64


class DriveToLabelStudio:
    def __init__(self, service_account_path: str):
        """Initialize Google Drive connection."""
        creds = service_account.Credentials.from_service_account_file(
            service_account_path, scopes=["https://www.googleapis.com/auth/drive"]
        )
        self.drive = build("drive", "v3", credentials=creds)

    @staticmethod
    def extract_drive_id(url_or_id: str) -> str:
        """Extract file/folder ID from Google Drive URL or return ID if already extracted.

        Supports:
        - https://drive.google.com/file/d/FILE_ID/view
        - https://drive.google.com/drive/folders/FOLDER_ID
        - https://drive.google.com/open?id=FILE_ID
        - Raw IDs
        """
        # If it's already an ID (no slashes or http), return as-is
        if not ("/" in url_or_id or "http" in url_or_id.lower()):
            return url_or_id

        # Extract from file URL: /d/FILE_ID/
        match = re.search(r"/d/([a-zA-Z0-9_-]+)", url_or_id)
        if match:
            return match.group(1)

        # Extract from folder URL: /folders/FOLDER_ID
        match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url_or_id)
        if match:
            return match.group(1)

        # Extract from open URL: ?id=FILE_ID
        match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url_or_id)
        if match:
            return match.group(1)

        # If no pattern matches, return original (might be malformed)
        return url_or_id

    def download_file_bytes(self, file_id: str) -> bytes:
        """Download file content as bytes."""
        # Ensure we have just the ID
        clean_id = self.extract_drive_id(file_id)

        request = self.drive.files().get_media(fileId=clean_id, supportsAllDrives=True)
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer.read()

    def read_jsonl(self, file_id: str) -> List[Dict[str, Any]]:
        """Read JSONL metadata file from Drive."""
        content = self.download_file_bytes(file_id).decode("utf-8")
        records = []
        for line in content.splitlines():
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON line: {e}")
                    continue
        return records

    def find_file_id(self, folder_id: str, filename: str) -> str:
        """Find file ID by name in folder."""
        # Ensure we have just the folder ID
        clean_folder_id = self.extract_drive_id(folder_id)

        query = f"'{clean_folder_id}' in parents and name = '{filename}' and trashed = false"
        response = (
            self.drive.files()
            .list(
                q=query,
                spaces="drive",
                fields="files(id,name)",
                pageSize=1,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = response.get("files", [])
        print(f"Searching for file '{filename}' in folder '{clean_folder_id}': Found {len(files)} matches.")
        return files[0]["id"] if files else None

    def image_to_data_url(self, file_id: str) -> str:
        """Convert image to data URL for Label Studio."""
        image_bytes = self.download_file_bytes(file_id)
        encoded = base64.b64encode(image_bytes).decode("utf-8")
        return f"data:image/jpeg;base64,{encoded}"

    def prepare_label_studio_tasks(
        self,
        jsonl_id: str,
        hypo_folder_id: str,
        adv_folder_id: str,
        category: str,
        output_file: str = "tasks.json",
    ):
        """Convert Google Drive data to Label Studio task format."""
        print(f"Processing category: {category}")
        print(f"  JSONL ID: {self.extract_drive_id(jsonl_id)}")
        print(f"  Hypo folder ID: {self.extract_drive_id(hypo_folder_id)}")
        print(f"  Adv folder ID: {self.extract_drive_id(adv_folder_id)}")

        metadata = self.read_jsonl(jsonl_id)
        tasks = []

        print(f"Processing {len(metadata)} records for category: {category}")

        for idx, record in enumerate(metadata):
            hypo_id = record.get("hypo_id", "")
            adv_id = record.get("adversarial_id", "")

            if not hypo_id or not adv_id:
                print(f"Warning: Missing IDs in record {idx}")
                continue

            # Find image files
            hypo_file_id = self.find_file_id(hypo_folder_id, hypo_id)
            adv_file_id = self.find_file_id(adv_folder_id, adv_id)

            if not hypo_file_id or not adv_file_id:
                print(
                    f"Warning: Missing images for {record.get('id', idx)} (hypo: {hypo_id}, adv: {adv_id})"
                )
                continue

            # Create task
            task = {
                "data": {
                    "id": record.get("id", f"{category}_{idx}"),
                    "text": record.get("text", ""),
                    "hypothesis": record.get("hypothesis", ""),
                    "adversarial": record.get("adversarial", ""),
                    "hypo_image_url": self.image_to_data_url(hypo_file_id),
                    "adv_image_url": self.image_to_data_url(adv_file_id),
                    "hypo_id": hypo_id,
                    "adv_id": adv_id,
                    "category": category,
                    "pair_key": f"{hypo_id}|{adv_id}",
                }
            }

            tasks.append(task)

            if (idx + 1) % 10 == 0:
                print(f"Processed {idx + 1}/{len(metadata)} records")

        # Save tasks
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)

        print(f"✓ Created {len(tasks)} tasks in {output_file}")
        return tasks


# Example usage
if __name__ == "__main__":
    # Configuration
    SERVICE_ACCOUNT_PATH = "sheets-connect-374716-a206b590609a.json"

    # Category configurations - Can use full URLs or just IDs
    CATEGORIES = {
        "demography": {
            "jsonl_id": "https://drive.google.com/file/d/1G-LMafYzvBTEsyFnMCeU1rwr7C-hZ0bT/view?usp=drive_link",
            "hypo_folder": "https://drive.google.com/drive/folders/1CkUxRFl1R1-0Kc-C6KLZim8pVWWG6fDw?usp=drive_link",
            "adv_folder": "https://drive.google.com/drive/folders/1If1mId-e8jHYd_FsM8d7iyY1vNgNPH6n?usp=drive_link",
        },
        "animal": {
            "jsonl_id": "https://drive.google.com/file/d/1ylO1ElyR5TtOaAsSYC3DLG9VKvtuudZ6/view?usp=drive_link",
            "hypo_folder": "https://drive.google.com/drive/folders/1E_44-tKC5yg-1Lsqod2Uf5jNUw3u7888?usp=drive_link",
            "adv_folder": "https://drive.google.com/drive/folders/11XtDkPac2QL9F45CP3cxTOiW35JjvJAi?usp=drive_link",
        },
        "objects": {
            # Now supports full URLs - will extract IDs automatically
            "jsonl_id": "https://drive.google.com/file/d/1ECF7RIb8kOKyku8_B_W5ZMKgQhKAcQoC/view?usp=drive_link",
            "hypo_folder": "https://drive.google.com/drive/folders/1_0Gcb1gU4jIbBGv0wVO8fMvCgQpgm7dR?usp=drive_link",
            "adv_folder": "https://drive.google.com/drive/folders/1YU7P2KGHLX6FlcJzPRMknM_Z4lzY0MZC?usp=drive_link",
        },
    }

    converter = DriveToLabelStudio(SERVICE_ACCOUNT_PATH)

    # Prepare tasks for each category
    for category, config in CATEGORIES.items():
        output_file = f"tasks_{category}.json"
        try:
            converter.prepare_label_studio_tasks(
                jsonl_id=config["jsonl_id"],
                hypo_folder_id=config["hypo_folder"],
                adv_folder_id=config["adv_folder"],
                category=category,
                output_file=output_file,
            )
        except Exception as e:
            print(f"✗ Error processing {category}: {e}")
            import traceback

            traceback.print_exc()
