from dotenv import load_dotenv
import tempfile
from pathlib import Path


load_dotenv()

CSV_NAME_FORMAT = '{code}_{table}_{date}.csv'

DATE_FORMAT = '%d.%m.%Y'
CSV_DATE_FORMAT = '%Y%m%d'

# sources = MetaDb.

# DATA_DIR = Path(tempfile.gettempdir()) / 'kzt_data'