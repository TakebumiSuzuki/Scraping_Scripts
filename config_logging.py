import logging.config

LOGGING_CONFIG = {
    'version': 1,

    # Set to False to prevent disabling loggers from imported libraries.
    # この設定が True の場合、この設定ファイルを読み込む前に存在していた全てのロガーが無効化されます。
    # False に設定することで、インポートしたライブラリなどが独自に設定しているロガーを無効化せず、そのまま利用できます
    'disable_existing_loggers': False,

    'formatters': {
        # ファイル出力用
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        #コンソール出力用
        'simple': {
            'format': '%(levelname)s - %(message)s',
        },
    },

    'handlers': {
        # Handler for printing logs to the console (standard output).
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'INFO',  # Log INFO level and above to the console.
            'formatter': 'simple',
            'stream': 'ext://sys.stdout',  # Explicitly direct to stdout
        },
        # Handler for writing logs to a file, with rotation.
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'level': 'DEBUG', # Log DEBUG level and above to the file.
            'formatter': 'standard',
            'filename': 'logs/app.log', # Will be created in a 'logs' directory.
            'maxBytes': 1024 * 1024 * 5,  # 5 MB
            'backupCount': 5, # Keep up to 5 old log files.
            'encoding': 'utf-8',
        },
    },

    'loggers': {
        'root': {
            'level': 'DEBUG', # The lowest threshold for the logger itself.
            'handlers': ['console', 'file'],
        },

        # Example of a specific logger for a third-party library.
        # This prevents overly verbose logs from playwright.
        # Playwrightライブラリが内部でログを出す際に使用しているのと同じ名前 ('playwright') を指定することで、
        # そのロガーの設定を上書き・制御する。Playwrightの作者は、'playwright' という名前を付けて使っている。
        # 「PlaywrightのINFOレベルのログは不要だけど、WARNING以上は知りたい」という事。
        # そして、propagateはデフォルトでTrueに設定されているので、Falseに変更する。
        'playwright': {
            'level': 'WARNING',
            'handlers': ['console', 'file'],
            'propagate': False, # Don't pass these logs to the root logger.
        },
    },
}

def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)