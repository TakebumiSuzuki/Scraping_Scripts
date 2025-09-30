FROM mcr.microsoft.com/playwright/python:v1.55.0-noble
# タグの中にあるv1.55.0と、requirements.txtにある playwrightのバージョンを必ず一致させること！

WORKDIR /app

COPY requirements.txt ./

# rootユーザーとして実行されたプロセス(pip install)がシステムディレクトリにファイルを作成・変更する場合、
# その作成されたファイルやディレクトリの所有者はrootユーザーとなり、アクセス権限もrootユーザーに
# 読み書き・実行が許可される設定（例: rwxr-xr-x）になるのが一般的。しかし、本件では、
# ライブラリの利用（インポートして機能を使う）には読み込み権限があれば十分なので、これで問題はない。
RUN pip install --no-cache-dir -r requirements.txt

# Playwrightの公式Dockerイメージには、Playwrightとその対応ブラウザが既にインストールされているので以下は不要。
# 但し、python:3.11-slimなどのイメージをベースとした場合には下の行は必要。
# RUN playwright install --with-deps


# Playwrightの公式Dockerイメージには、pwuserという名前の非rootユーザーが最初から用意されている。
COPY --chown=pwuser:pwuser . .

# 5. ユーザーをpwuserに切り替え
USER pwuser

# 6. デフォルトのコマンドを指定（例としてbash）
CMD ["bash"]