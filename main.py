import sys
from datetime import datetime

import logging
from config_logging import setup_logging

from step1_extract_seed_urls.run import execute as step1
from step2_crawl_all_urls.run import execute as step2
from step3_delete_duplicated_urls.run import execute as step3
from step4_scrape_and_save.run import execute as step4
from step5_create_and_save_chunked_json.run import execute as step5
from step6_save_chunks_to_db.run import execute as step6
from step7_create_and_save_embeddings.run import execute as step7

setup_logging()
logger = logging.getLogger(__name__)


def main(run_id: str | None = None):
    # 1. 実行IDを決定する
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')

    interaction_dir = f"outputs/{run_id}"

    print(f"--- Starting execution with Run ID: {run_id} ---")
    step1(interaction_dir)
    step2(interaction_dir)
    step3(interaction_dir)
    step4(interaction_dir)
    step5(interaction_dir)



if __name__ == "__main__":
    # コマンドラインから、引数として run_id を入れられるようにしている。
    # 引数に何も入れない場合は、デフォルト(None)となり、結果、フォルダ名は日付時間となる
    run_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
    main(run_id=run_id_arg)