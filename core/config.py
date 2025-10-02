# Config with local golden copies
RAW_DATA_PATH = "data/raw/credit_card_process_activities.csv"
GOLDEN_STAGE_PATH = "data/golden/golden_stage_master.csv"
GOLDEN_APP_PATH   = "data/golden/golden_application_master.csv"
# Outputs remain exactly as your original script
OUTPUT_STAGE_PATH = "/content/drive/MyDrive/data sets/output_stage_master.csv"
OUTPUT_APP_PATH   = "/content/drive/MyDrive/data sets/output_application_master.csv"


# API Config
# Max upload size: 25 MB. Adjust as needed.
MAX_CONTENT_LENGTH = int(os.getenv("MAX_CONTENT_LENGTH", 25 * 1024 * 1024))
# Allowed mime types for CSV
ALLOWED_MIME_TYPES = {"text/csv", "application/vnd.ms-excel", "application/csv"}
# Simple token auth (optional but recommended). Set CSV_API_TOKEN in env.
API_TOKEN = os.getenv("CSV_API_TOKEN")
# Folder to store uploads (optional; we also support streaming without saving)
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", "./uploads"))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
