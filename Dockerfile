# より安定したDebian 11 (bullseye) ベースのイメージを使用
FROM python:3.12-bullseye

# ffmpegを確実にインストール
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

# アプリケーションのコードと依存関係をコピー
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# ffmpegがインストールされているかビルド時に確認（保険）
RUN which ffmpeg

# Gunicornでサービスを実行
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
