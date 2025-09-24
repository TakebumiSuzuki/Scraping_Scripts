import sys
from datetime import datetime

import logging
from config_logging import setup_logging


# ★ アプリケーションの開始地点で一度だけ設定を読み込む
setup_logging()
logger = logging.getLogger(__name__)

def main(run_id: str | None = None):
    # 1. 実行IDを決定する
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')

    output_dir = f"outputs/{run_id}"

    print(f"--- Starting execution with Run ID: {run_id} ---")



if __name__ == "__main__":
    run_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(run_id=run_id_arg)