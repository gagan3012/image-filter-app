"""
MLLM-based Image Filtering Script
Automates the annotation process using Qwen-VL or similar vision-language models
Outputs Label Studio compatible format with only accepted pairs
"""

"""
To run 
python qwen3_filter.py \
    --category animal \
    --service-account path/to/service_account.json \
    --model Qwen/Qwen3-VL-8B-Instruct \
    --temperature 0.0 \
    --max-tokens 256
"""

import io
import json
import argparse
import base64
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from tqdm import tqdm

import torch
from qwen_vl_utils import process_vision_info
from transformers import AutoProcessor
from vllm import LLM, SamplingParams

# Google Drive API imports
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from PIL import Image

import os

os.environ["VLLM_WORKER_MULTIPROC_METHOD"] = "spawn"

# ==================== Configuration ====================
CATEGORY_CONFIGS = {
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
        "jsonl_id": "https://drive.google.com/file/d/1ECF7RIb8kOKyku8_B_W5ZMKgQhKAcQoC/view?usp=drive_link",
        "hypo_folder": "https://drive.google.com/drive/folders/1_0Gcb1gU4jIbBGv0wVO8fMvCgQpgm7dR?usp=drive_link",
        "adv_folder": "https://drive.google.com/drive/folders/1YU7P2KGHLX6FlcJzPRMknM_Z4lzY0MZC?usp=drive_link",
    },
}


# ==================== Drive Helper Functions ====================
def extract_drive_id(url_or_id: str) -> str:
    """Extract file/folder ID from Google Drive URL."""
    import re

    if not ("/" in url_or_id or "http" in url_or_id.lower()):
        return url_or_id

    match = re.search(r"/d/([a-zA-Z0-9_-]+)", url_or_id)
    if match:
        return match.group(1)

    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", url_or_id)
    if match:
        return match.group(1)

    match = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url_or_id)
    if match:
        return match.group(1)

    return url_or_id


def get_drive_service(service_account_json: str):
    """Initialize Google Drive service with service account credentials."""
    with open(service_account_json, "r") as f:
        sa_info = json.load(f)

    creds = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def download_file_from_drive(drive_service, file_id: str) -> bytes:
    """Download file content from Google Drive."""
    clean_id = extract_drive_id(file_id)
    request = drive_service.files().get_media(fileId=clean_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer.read()


def read_jsonl_from_drive(drive_service, file_id: str) -> List[Dict[str, Any]]:
    """Read and parse JSONL file from Google Drive."""
    clean_id = extract_drive_id(file_id)
    content = download_file_from_drive(drive_service, clean_id)
    text = content.decode("utf-8", errors="ignore")

    records = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    return records


def find_file_id_in_folder(
    drive_service, folder_id: str, filename: str
) -> Optional[str]:
    """Find file ID by name within a specific folder."""
    if not filename:
        return None

    clean_folder_id = extract_drive_id(folder_id)
    query = (
        f"'{clean_folder_id}' in parents and name = '{filename}' and trashed = false"
    )
    response = (
        drive_service.files()
        .list(
            q=query,
            spaces="drive",
            fields="files(id,name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )

    files = response.get("files", [])
    return files[0]["id"] if files else None


def save_image_locally(drive_service, file_id: str, output_path: str) -> bool:
    """Download image from Drive and save locally."""
    try:
        content = download_file_from_drive(drive_service, file_id)
        with open(output_path, "wb") as f:
            f.write(content)
        return True
    except Exception as e:
        print(f"Error downloading image {file_id}: {e}")
        return False


def image_to_data_url(image_path: str) -> str:
    """Convert local image to base64 data URL for Label Studio."""
    with open(image_path, "rb") as f:
        image_bytes = f.read()
    encoded = base64.b64encode(image_bytes).decode("utf-8")

    # Detect image format
    img_format = "jpeg"
    if image_path.lower().endswith(".png"):
        img_format = "png"
    elif image_path.lower().endswith(".webp"):
        img_format = "webp"

    return f"data:image/{img_format};base64,{encoded}"


# ==================== MLLM Processing ====================
def prepare_inputs_for_vllm(messages: List[Dict], processor):
    """Prepare inputs for VLLM inference."""
    text = processor.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True
    )

    image_inputs, video_inputs, video_kwargs = process_vision_info(
        messages,
        image_patch_size=processor.image_processor.patch_size,
        return_video_kwargs=True,
        return_video_metadata=True,
    )

    mm_data = {}
    if image_inputs is not None:
        mm_data["image"] = image_inputs
    if video_inputs is not None:
        mm_data["video"] = video_inputs

    return {
        "prompt": text,
        "multi_modal_data": mm_data,
        "mm_processor_kwargs": video_kwargs,
    }


def create_evaluation_prompt(
    text: str, hypothesis: str, adversarial: str, image_type: str
) -> str:
    """
    Create a detailed prompt for the MLLM to evaluate image quality.

    Args:
        text: The original text description
        hypothesis: Hypothesis caption (non-prototype)
        adversarial: Adversarial caption (prototype)
        image_type: Either 'hypothesis' or 'adversarial'
    """
    caption = hypothesis if image_type == "hypothesis" else adversarial

    prompt = f"""You are an expert image quality annotator. Your task is to evaluate whether a generated image correctly matches its intended caption and the original text context.

**Original Text**: {text}

**Image Caption** ({image_type}): {caption}

**Evaluation Criteria**:
1. **Semantic Accuracy**: Does the image accurately represent the caption?
2. **Visual Quality**: Is the image clear, well-rendered, and free of artifacts?
3. **Context Alignment**: Does the image make sense in the context of the original text?
4. **Object/Subject Correctness**: Are the main subjects/objects correctly depicted?
5. **Attribute Accuracy**: Are attributes (colors, sizes, positions, demographics, etc.) correct?

**Instructions**:
- Carefully examine the provided image
- Compare it against the caption and original text
- Consider all evaluation criteria
- Provide a decision: ACCEPT or REJECT

**Response Format**:
Decision: [ACCEPT/REJECT]
Reasoning: [Brief explanation of your decision]

Respond now:"""

    return prompt


def evaluate_image_with_mllm(
    llm: LLM,
    processor,
    sampling_params: SamplingParams,
    image_path: str,
    text: str,
    hypothesis: str,
    adversarial: str,
    image_type: str,
) -> Tuple[str, str]:
    """
    Evaluate a single image using the MLLM.

    Returns:
        Tuple of (decision, reasoning) where decision is 'accepted' or 'rejected'
    """
    prompt_text = create_evaluation_prompt(text, hypothesis, adversarial, image_type)

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "image": image_path,
                },
                {"type": "text", "text": prompt_text},
            ],
        }
    ]

    inputs = [prepare_inputs_for_vllm(messages, processor)]
    outputs = llm.generate(inputs, sampling_params=sampling_params)

    generated_text = outputs[0].outputs[0].text

    # Parse the response
    decision = "rejected"  # default
    reasoning = generated_text

    # Extract decision from response
    lower_text = generated_text.lower()
    if "decision:" in lower_text:
        decision_line = (
            generated_text.split("Decision:")[1].split("\n")[0].strip().lower()
        )
        if "accept" in decision_line:
            decision = "accepted"
        elif "reject" in decision_line:
            decision = "rejected"
    elif "accept" in lower_text[:100]:  # Check first 100 chars
        decision = "accepted"

    return decision, reasoning


# ==================== Main Processing Pipeline ====================
def process_category(
    category: str,
    drive_service,
    llm: LLM,
    processor,
    sampling_params: SamplingParams,
    output_dir: str,
    start_idx: int = 0,
    end_idx: Optional[int] = None,
    batch_size: int = 1,
):
    """
    Process all records in a category using MLLM evaluation.
    Saves results in Label Studio format with only BOTH-ACCEPTED pairs.
    """
    config = CATEGORY_CONFIGS[category]
    output_path = Path(output_dir) / category
    output_path.mkdir(parents=True, exist_ok=True)

    # Create subdirectories for images
    hypo_img_dir = output_path / "hypothesis_images"
    adv_img_dir = output_path / "adversarial_images"
    hypo_img_dir.mkdir(exist_ok=True)
    adv_img_dir.mkdir(exist_ok=True)

    # Load metadata
    print(f"Loading metadata for {category}...")
    records = read_jsonl_from_drive(drive_service, config["jsonl_id"])

    if end_idx is None:
        end_idx = len(records)

    records = records[start_idx:end_idx]
    print(f"Processing {len(records)} records (index {start_idx} to {end_idx})")

    # Results storage
    all_results = []  # For detailed JSONL output
    label_studio_tasks = []  # For Label Studio import (only accepted pairs)

    for idx, record in enumerate(tqdm(records, desc=f"Processing {category}")):
        record_id = record.get("id", f"unknown_{idx}")
        text = record.get("text", "")
        hypothesis = record.get("hypothesis", "")
        adversarial = record.get("adversarial", "")
        hypo_filename = record.get("hypo_id", "")
        adv_filename = record.get("adversarial_id", "")

        result = {
            "record_id": record_id,
            "index": start_idx + idx,
            "text": text,
            "hypothesis_caption": hypothesis,
            "adversarial_caption": adversarial,
            "hypo_filename": hypo_filename,
            "adv_filename": adv_filename,
            "category": category,
            "pair_key": f"{hypo_filename}|{adv_filename}",
        }

        hypo_local_path = None
        adv_local_path = None

        # Process Hypothesis Image
        hypo_file_id = find_file_id_in_folder(
            drive_service, config["hypo_folder"], hypo_filename
        )

        if hypo_file_id:
            hypo_local_path = str(hypo_img_dir / hypo_filename)
            if save_image_locally(drive_service, hypo_file_id, hypo_local_path):
                hypo_decision, hypo_reasoning = evaluate_image_with_mllm(
                    llm,
                    processor,
                    sampling_params,
                    hypo_local_path,
                    text,
                    hypothesis,
                    adversarial,
                    "hypothesis",
                )
                result["hypothesis_decision"] = hypo_decision
                result["hypothesis_reasoning"] = hypo_reasoning
            else:
                result["hypothesis_decision"] = "error"
                result["hypothesis_reasoning"] = "Failed to download image"
        else:
            result["hypothesis_decision"] = "missing"
            result["hypothesis_reasoning"] = "Image not found in Drive"

        # Process Adversarial Image
        adv_file_id = find_file_id_in_folder(
            drive_service, config["adv_folder"], adv_filename
        )

        if adv_file_id:
            adv_local_path = str(adv_img_dir / adv_filename)
            if save_image_locally(drive_service, adv_file_id, adv_local_path):
                adv_decision, adv_reasoning = evaluate_image_with_mllm(
                    llm,
                    processor,
                    sampling_params,
                    adv_local_path,
                    text,
                    hypothesis,
                    adversarial,
                    "adversarial",
                )
                result["adversarial_decision"] = adv_decision
                result["adversarial_reasoning"] = adv_reasoning
            else:
                result["adversarial_decision"] = "error"
                result["adversarial_reasoning"] = "Failed to download image"
        else:
            result["adversarial_decision"] = "missing"
            result["adversarial_reasoning"] = "Image not found in Drive"

        all_results.append(result)

        # ========== CREATE LABEL STUDIO TASK ONLY IF BOTH ACCEPTED ==========
        if (
            result.get("hypothesis_decision") == "accepted"
            and result.get("adversarial_decision") == "accepted"
            and hypo_local_path
            and adv_local_path
        ):
            try:
                label_studio_task = {
                    "data": {
                        "id": record_id,
                        "text": text,
                        "hypothesis": hypothesis,
                        "adversarial": adversarial,
                        "hypo_image_url": image_to_data_url(hypo_local_path),
                        "adv_image_url": image_to_data_url(adv_local_path),
                        "hypo_id": hypo_filename,
                        "adv_id": adv_filename,
                        "category": category,
                        "pair_key": result["pair_key"],
                    },
                    "predictions": [
                        {
                            "model_version": "mllm_filter_v1",
                            "result": [
                                {
                                    "from_name": "hypo_decision",
                                    "to_name": "hypo_image",
                                    "type": "choices",
                                    "value": {"choices": ["accepted"]},
                                },
                                {
                                    "from_name": "adv_decision",
                                    "to_name": "adv_image",
                                    "type": "choices",
                                    "value": {"choices": ["accepted"]},
                                },
                            ],
                        }
                    ],
                    "meta": {
                        "hypo_reasoning": result["hypothesis_reasoning"],
                        "adv_reasoning": result["adversarial_reasoning"],
                    },
                }
                label_studio_tasks.append(label_studio_task)
            except Exception as e:
                print(
                    f"Warning: Failed to create Label Studio task for {record_id}: {e}"
                )

        # Save intermediate results every 10 records
        if (idx + 1) % 10 == 0:
            save_results(
                all_results,
                output_path
                / f"detailed_results_checkpoint_{start_idx}_{idx + 1}.jsonl",
            )
            save_label_studio_tasks(
                label_studio_tasks,
                output_path
                / f"label_studio_accepted_checkpoint_{start_idx}_{idx + 1}.json",
            )

    # Save final results
    final_detailed = output_path / f"detailed_results_{start_idx}_{end_idx}.jsonl"
    final_label_studio = (
        output_path / f"label_studio_accepted_{start_idx}_{end_idx}.json"
    )

    save_results(all_results, final_detailed)
    save_label_studio_tasks(label_studio_tasks, final_label_studio)

    print(f"\n✓ Detailed results saved to {final_detailed}")
    print(f"✓ Label Studio tasks (BOTH ACCEPTED ONLY) saved to {final_label_studio}")

    # Print summary statistics
    print_summary(all_results, label_studio_tasks)

    return all_results, label_studio_tasks


def save_results(results: List[Dict], output_path: Path):
    """Save detailed results to JSONL file."""
    with open(output_path, "w", encoding="utf-8") as f:
        for result in results:
            f.write(json.dumps(result, ensure_ascii=False) + "\n")


def save_label_studio_tasks(tasks: List[Dict], output_path: Path):
    """Save Label Studio tasks to JSON file."""
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)


def print_summary(all_results: List[Dict], label_studio_tasks: List[Dict]):
    """Print summary statistics of the evaluation."""
    total = len(all_results)
    hypo_accepted = sum(
        1 for r in all_results if r.get("hypothesis_decision") == "accepted"
    )
    hypo_rejected = sum(
        1 for r in all_results if r.get("hypothesis_decision") == "rejected"
    )
    adv_accepted = sum(
        1 for r in all_results if r.get("adversarial_decision") == "accepted"
    )
    adv_rejected = sum(
        1 for r in all_results if r.get("adversarial_decision") == "rejected"
    )
    both_accepted = len(label_studio_tasks)

    print("\n" + "=" * 60)
    print("EVALUATION SUMMARY")
    print("=" * 60)
    print(f"Total records processed: {total}")
    print(f"\nHypothesis Images:")
    print(f"  Accepted: {hypo_accepted} ({hypo_accepted / total * 100:.1f}%)")
    print(f"  Rejected: {hypo_rejected} ({hypo_rejected / total * 100:.1f}%)")
    print(f"\nAdversarial Images:")
    print(f"  Accepted: {adv_accepted} ({adv_accepted / total * 100:.1f}%)")
    print(f"  Rejected: {adv_rejected} ({adv_rejected / total * 100:.1f}%)")
    print(
        f"\n🎯 BOTH ACCEPTED (Label Studio ready): {both_accepted} ({both_accepted / total * 100:.1f}%)"
    )
    print("=" * 60)


# ==================== Main Entry Point ====================
def main():
    parser = argparse.ArgumentParser(
        description="MLLM-based Image Filtering with Label Studio Export"
    )
    parser.add_argument(
        "--category",
        type=str,
        required=True,
        choices=["demography", "animal", "objects"],
        help="Category to process",
    )
    parser.add_argument(
        "--service-account",
        type=str,
        required=True,
        help="Path to Google service account JSON file",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="Qwen/Qwen2-VL-72B-Instruct",
        help="MLLM model checkpoint path",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./mllm_results",
        help="Output directory for results",
    )
    parser.add_argument(
        "--start-idx", type=int, default=0, help="Starting index for processing"
    )
    parser.add_argument(
        "--end-idx",
        type=int,
        default=None,
        help="Ending index for processing (None for all)",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="Sampling temperature (0 for deterministic)",
    )
    parser.add_argument(
        "--max-tokens", type=int, default=512, help="Maximum tokens for generation"
    )

    args = parser.parse_args()

    # Initialize Drive service
    print("Initializing Google Drive service...")
    drive_service = get_drive_service(args.service_account)

    # Initialize MLLM
    print(f"Loading model {args.model}...")
    processor = AutoProcessor.from_pretrained(args.model)

    llm = LLM(
        model=args.model,
        tensor_parallel_size=torch.cuda.device_count(),
        seed=42,
        trust_remote_code=True,
    )

    sampling_params = SamplingParams(
        temperature=args.temperature,
        max_tokens=args.max_tokens,
        top_k=-1,
        stop_token_ids=[],
    )

    # Process category
    print(f"\nProcessing category: {args.category}")
    all_results, label_studio_tasks = process_category(
        category=args.category,
        drive_service=drive_service,
        llm=llm,
        processor=processor,
        sampling_params=sampling_params,
        output_dir=args.output_dir,
        start_idx=args.start_idx,
        end_idx=args.end_idx,
    )

    print("\n✅ Processing complete!")
    print(f"📊 {len(label_studio_tasks)} pairs ready for Label Studio (both accepted)")


if __name__ == "__main__":
    main()
