import sys
from datetime import datetime
from pathlib import Path

import logging
from config_logging import setup_logging


# ★ アプリケーションの開始地点で一度だけ設定を読み込む
setup_logging()

logger = logging.getLogger(__name__)

def main(run_id: str | None = None):
    # 1. 実行IDを決定する
    if run_id is None:
        # YYYYMMDD_HHMMSS 形式（秒まで入れるとより安全）
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')

    print(f"--- Starting execution with Run ID: {run_id} ---")

    # 2. この実行IDに基づいた成果物保存ディレクトリを作成する
    project_root = Path(__file__).parent
    output_dir = project_root / f"outputs_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Outputs will be saved to: {output_dir}")

    # 3. 各ステップに「実行ID」ではなく「保存先ディレクトリのパス」を渡す
    #    (こちらの方が、各ステップの責務がより明確になります)
    # step1_result = step1_process(..., output_dir=output_dir)
    # step2_result = step2_process(step1_result, output_dir=output_dir)

’


if __name__ == "__main__":
    run_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(run_id=run_id_arg)