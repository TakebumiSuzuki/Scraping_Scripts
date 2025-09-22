import logging
from config_logging import setup_logging

# ★ アプリケーションの開始地点で一度だけ設定を読み込む
setup_logging()

logger = logging.getLogger(__name__)

def main():
    pass


if __name__ == "__main__":
    main()