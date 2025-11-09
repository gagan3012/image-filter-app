import json
import base64
from pathlib import Path
from typing import List, Dict, Any


class LocalToLabelStudio:
    """Convert local images and metadata to Label Studio format."""

    def __init__(self, data_root: str):
        """Initialize with root data directory.

        Args:
            data_root: Path to root directory containing category folders
        """
        self.data_root = Path(data_root)
        if not self.data_root.exists():
            raise ValueError(f"Data root directory not found: {data_root}")

    def read_jsonl(self, jsonl_path: Path) -> List[Dict[str, Any]]:
        """Read JSONL metadata file from local filesystem."""
        if not jsonl_path.exists():
            raise FileNotFoundError(f"JSONL file not found: {jsonl_path}")

        records = []
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Skipping invalid JSON at line {line_num}: {e}")

        return records

    def image_to_data_url(self, image_path: Path) -> str:
        """Convert local image to base64 data URL for Label Studio."""
        if not image_path.exists():
            raise FileNotFoundError(f"Image not found: {image_path}")

        with open(image_path, "rb") as f:
            image_bytes = f.read()

        encoded = base64.b64encode(image_bytes).decode("utf-8")

        # Detect image format
        suffix = image_path.suffix.lower()
        img_format = {
            ".jpg": "jpeg",
            ".jpeg": "jpeg",
            ".png": "png",
            ".webp": "webp",
            ".gif": "gif",
        }.get(suffix, "jpeg")

        return f"data:image/{img_format};base64,{encoded}"

    def prepare_label_studio_tasks(
        self,
        jsonl_path: str,
        hypo_folder: str,
        adv_folder: str,
        category: str,
        output_file: str = "tasks.json",
    ) -> List[Dict[str, Any]]:
        """Convert local data to Label Studio task format.

        Args:
            jsonl_path: Path to metadata JSONL file (relative to data_root or absolute)
            hypo_folder: Path to hypothesis images folder (relative to data_root or absolute)
            adv_folder: Path to adversarial images folder (relative to data_root or absolute)
            category: Category name (e.g., 'animal', 'demography')
            output_file: Output JSON file name

        Returns:
            List of Label Studio tasks
        """
        # Convert to Path objects
        jsonl_path = Path(jsonl_path)
        hypo_folder = Path(hypo_folder)
        adv_folder = Path(adv_folder)

        # Make absolute if relative
        if not jsonl_path.is_absolute():
            jsonl_path = self.data_root / jsonl_path
        if not hypo_folder.is_absolute():
            hypo_folder = self.data_root / hypo_folder
        if not adv_folder.is_absolute():
            adv_folder = self.data_root / adv_folder

        print(f"\nProcessing category: {category}")
        print(f"  JSONL: {jsonl_path}")
        print(f"  Hypothesis folder: {hypo_folder}")
        print(f"  Adversarial folder: {adv_folder}")

        # Verify paths exist
        if not hypo_folder.exists():
            raise ValueError(f"Hypothesis folder not found: {hypo_folder}")
        if not adv_folder.exists():
            raise ValueError(f"Adversarial folder not found: {adv_folder}")

        # Read metadata
        metadata = self.read_jsonl(jsonl_path)
        print(f"  Found {len(metadata)} records in metadata")

        tasks = []
        missing_images = []

        for idx, record in enumerate(metadata):
            hypo_id = record.get("hypo_id", "")
            adv_id = record.get("adversarial_id", "")

            if not hypo_id or not adv_id:
                print(f"  Warning: Missing IDs in record {idx}")
                continue

            # Find image files
            hypo_path = hypo_folder / hypo_id
            adv_path = adv_folder / adv_id

            # Check if images exist
            if not hypo_path.exists():
                missing_images.append(f"Hypothesis: {hypo_id}")
                continue
            if not adv_path.exists():
                missing_images.append(f"Adversarial: {adv_id}")
                continue

            try:
                # Create task with embedded images
                task = {
                    "data": {
                        "id": record.get("id", f"{category}_{idx}"),
                        "text": record.get("text", ""),
                        "hypothesis": record.get("hypothesis", ""),
                        "adversarial": record.get("adversarial", ""),
                        "hypo_image_url": self.image_to_data_url(hypo_path),
                        "adv_image_url": self.image_to_data_url(adv_path),
                        "hypo_id": hypo_id,
                        "adv_id": adv_id,
                        "category": category,
                        "pair_key": f"{hypo_id}|{adv_id}",
                    }
                }

                tasks.append(task)

                if (idx + 1) % 10 == 0:
                    print(f"  Processed {idx + 1}/{len(metadata)} records")

            except Exception as e:
                print(f"  Error processing record {idx} ({hypo_id}, {adv_id}): {e}")
                continue

        # Save tasks
        output_path = Path(output_file)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)

        print(f"\n✓ Created {len(tasks)} tasks in {output_path}")

        if missing_images:
            print(f"⚠ Warning: {len(missing_images)} missing images")
            if len(missing_images) <= 10:
                for img in missing_images:
                    print(f"    - {img}")
            else:
                print(f"    (showing first 10)")
                for img in missing_images[:10]:
                    print(f"    - {img}")

        return tasks


# Example usage
if __name__ == "__main__":
    # Configuration - adjust paths to match your local structure
    DATA_ROOT = "./data"  # Root directory containing all data

    # Category configurations with local paths
    CATEGORIES = {
        "demography": {
            "jsonl_path": "demography/metadata.jsonl",  # Relative to DATA_ROOT
            "hypo_folder": "demography/hypothesis_images",
            "adv_folder": "demography/adversarial_images",
        },
        "animal": {
            "jsonl_path": "animal/metadata.jsonl",
            "hypo_folder": "animal/hypothesis_images",
            "adv_folder": "animal/adversarial_images",
        },
        "objects": {
            "jsonl_path": "objects/metadata.jsonl",
            "hypo_folder": "objects/hypothesis_images",
            "adv_folder": "objects/adversarial_images",
        },
    }

    converter = LocalToLabelStudio(DATA_ROOT)

    # Prepare tasks for each category
    for category, config in CATEGORIES.items():
        output_file = f"tasks_{category}.json"
        try:
            converter.prepare_label_studio_tasks(
                jsonl_path=config["jsonl_path"],
                hypo_folder=config["hypo_folder"],
                adv_folder=config["adv_folder"],
                category=category,
                output_file=output_file,
            )
        except Exception as e:
            print(f"✗ Error processing {category}: {e}")
            import traceback

            traceback.print_exc()

    print("\n✅ All categories processed!")
