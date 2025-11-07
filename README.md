# Image Filter - MLLM Annotation Pipeline

Automated image filtering pipeline using MLLMs (Qwen-VL) with Label Studio for human review.

## 🚀 Quick Start

### 1. Run MLLM Filter

Filter image pairs using the vision-language model:

```bash
python qwen3_filter.py \
    --category animal \
    --service-account path/to/service_account.json \
    --model Qwen/Qwen3-VL-8B-Instruct \
    --start-idx 0 \
    --end-idx 100
```

**Output**:

- `detailed_results_{start}_{end}.jsonl` - All evaluations
- `label_studio_accepted_{start}_{end}.json` - Both-accepted pairs only

### 2. Setup Label Studio Interface

1. Go to [Label Studio Tag Playground](https://labelstud.io/playground/)
2. Copy content from `label_studio.xml`
3. Paste into the playground to preview the interface
4. Copy the validated XML configuration

### 3. Import to HuggingFace Label Studio

Visit: <https://huggingface.co/spaces/gagan3012/image-filter>

**Steps:**

1. Create new project or select existing
2. Go to **Settings** → **Labeling Interface**
3. Paste the XML from step 2
4. Go to **Import** tab
5. Upload `label_studio_accepted_{category}_{start}_{end}.json` files
6. Start annotating! 🎯
