import os
import asyncio
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime

import pandas as pd
from watchfiles import awatch

from aiobotocore.session import get_session
from botocore.client import Config
from botocore.exceptions import ClientError


# =========================
# Конфигурация из env vars
# =========================
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "https://s3.ru-3.storage.selcloud.ru")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "task3/processed/")           # куда грузим обработанные файлы
S3_LOG_KEY = os.getenv("S3_LOG_KEY", "task3/logs/pipeline.log")  # куда грузим лог (один и тот же key)
S3_VERIFY_SSL = os.getenv("S3_VERIFY_SSL", "0").strip() not in ("0", "false", "False", "")

WATCH_DIR = Path(os.getenv("WATCH_DIR", "./inbox")).resolve()
ARCHIVE_DIR = Path(os.getenv("ARCHIVE_DIR", "./archive")).resolve()
TMP_DIR = Path(os.getenv("TMP_DIR", "./tmp")).resolve()
LOG_DIR = Path(os.getenv("LOG_DIR", "./logs")).resolve()

ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
SECRET_KEY = os.getenv("S3_SECRET_KEY")


# =========================
# Логирование
# =========================
def setup_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / "pipeline.log"

    logger = logging.getLogger("task3_pipeline")
    logger.setLevel(logging.INFO)

    # чтобы не дублировать хэндлеры при перезапусках в IDE
    if logger.handlers:
        return logger

    handler = RotatingFileHandler(
        log_path, maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    return logger


LOGGER = setup_logger()


# =========================
# S3 Async Client (Selectel)
# =========================
class AsyncS3:
    def __init__(self):
        if not ACCESS_KEY or not SECRET_KEY or not S3_BUCKET:
            raise RuntimeError(
                "Не заданы S3_ACCESS_KEY / S3_SECRET_KEY / S3_BUCKET в переменных окружения."
            )

        self._session = get_session()
        self._auth = {
            "aws_access_key_id": ACCESS_KEY,
            "aws_secret_access_key": SECRET_KEY,
            "endpoint_url": S3_ENDPOINT,
            "region_name": "us-east-1",
            "config": Config(signature_version="s3v4"),
            "verify": S3_VERIFY_SSL,  # False, если у тебя self-signed цепочка
        }

    async def put_file(self, local_path: Path, key: str):
        async with self._session.create_client("s3", **self._auth) as s3:
            with local_path.open("rb") as f:
                await s3.put_object(Bucket=S3_BUCKET, Key=key, Body=f)

    async def put_bytes(self, data: bytes, key: str):
        async with self._session.create_client("s3", **self._auth) as s3:
            await s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)


# =========================
# Утилиты
# =========================
async def wait_file_stable(path: Path, checks: int = 5, delay: float = 0.4) -> None:
    """
    Ждём, пока файл перестанет меняться по размеру.
    Это защищает от ситуации, когда файл ещё дописывается.
    """
    last = -1
    stable_count = 0

    for _ in range(checks * 5):
        if not path.exists():
            return
        size = path.stat().st_size
        if size == last and size > 0:
            stable_count += 1
            if stable_count >= checks:
                return
        else:
            stable_count = 0
            last = size
        await asyncio.sleep(delay)


def process_csv(input_path: Path, tmp_dir: Path) -> Path:
    """
    Читаем CSV в pandas, делаем фильтрацию и сохраняем новый CSV во временную папку.
    Фильтрация (пример):
      - если есть колонка 'value' (числовая) -> берём value > 50
      - иначе берём только строки без пустот в первой колонке
    """
    df = pd.read_csv(input_path)

    if "value" in df.columns:
        # стараемся привести к числу, нечисловые станут NaN
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        out = df[df["value"] > 50].copy()
    else:
        first_col = df.columns[0]
        out = df[df[first_col].notna()].copy()

    tmp_dir.mkdir(parents=True, exist_ok=True)
    out_name = input_path.stem + "_processed.csv"
    out_path = tmp_dir / out_name
    out.to_csv(out_path, index=False)
    return out_path


def archive_file(src: Path, archive_dir: Path) -> Path:
    """
    Перемещаем исходный файл в archive/ с добавлением timestamp, чтобы не затирать.
    """
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = archive_dir / f"{src.stem}__arch_{ts}{src.suffix}"
    src.rename(dst)
    return dst


async def upload_log_to_s3(s3: AsyncS3, log_path: Path):
    """
    Загружаем лог в S3 одним и тем же ключом.
    Если в бакете включено версионирование — появятся версии лога.
    """
    if not log_path.exists():
        return
    await s3.put_file(log_path, S3_LOG_KEY)


# =========================
# Основной пайплайн
# =========================
async def handle_file(s3: AsyncS3, file_path: Path):
    """
    Обрабатывает один файл: стабилизация -> pandas -> tmp -> upload -> archive -> upload log.
    """
    try:
        LOGGER.info(f"🔎 Новый файл обнаружен: {file_path.name}")
        await wait_file_stable(file_path)

        if not file_path.exists():
            LOGGER.warning(f"Файл исчез до обработки: {file_path}")
            return

        if file_path.suffix.lower() != ".csv":
            LOGGER.info(f"Пропускаю (не CSV): {file_path.name}")
            return

        LOGGER.info("📥 Читаю и фильтрую через pandas...")
        processed_path = process_csv(file_path, TMP_DIR)
        LOGGER.info(f"✅ Сохранён обработанный файл: {processed_path.name}")

        # ключ в S3: префикс + имя обработанного файла
        s3_key = f"{S3_PREFIX}{processed_path.name}"
        LOGGER.info(f"☁️ Загружаю в S3: s3://{S3_BUCKET}/{s3_key}")
        await s3.put_file(processed_path, s3_key)
        LOGGER.info("✅ Загрузка в S3 успешна")

        archived = archive_file(file_path, ARCHIVE_DIR)
        LOGGER.info(f"📦 Исходник перемещён в архив: {archived.name}")

        # Перезаливаем лог в S3 (для версионирования лога)
        log_path = (LOG_DIR / "pipeline.log")
        LOGGER.info(f"🧾 Загружаю лог в S3: s3://{S3_BUCKET}/{S3_LOG_KEY}")
        await upload_log_to_s3(s3, log_path)
        LOGGER.info("✅ Лог загружен в S3")

    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        msg = e.response.get("Error", {}).get("Message", str(e))
        LOGGER.error(f"❌ S3 ClientError: {code} — {msg}")
    except Exception as e:
        LOGGER.exception(f"❌ Ошибка пайплайна: {e}")


async def main():
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    LOGGER.info("=== Task 3 pipeline started ===")
    LOGGER.info(f"Watching folder: {WATCH_DIR}")
    LOGGER.info(f"S3 endpoint: {S3_ENDPOINT}")
    LOGGER.info(f"S3 bucket:   {S3_BUCKET}")
    LOGGER.info(f"S3 prefix:   {S3_PREFIX}")
    LOGGER.info(f"SSL verify:  {S3_VERIFY_SSL}")

    s3 = AsyncS3()

    # бесконечный watcher
    async for changes in awatch(WATCH_DIR):
        # changes: set of (Change, path)
        for _, changed_path in changes:
            p = Path(changed_path)
            if p.is_file():
                # запускаем обработку в фоне (конкурентно)
                asyncio.create_task(handle_file(s3, p))


if __name__ == "__main__":
    asyncio.run(main())
