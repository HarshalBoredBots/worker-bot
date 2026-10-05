# Worker Rename Bot

A distributed Telegram file-rename worker bot that downloads media, injects metadata using FFmpeg/mkvpropedit, and re-uploads to an output channel. Designed to run as one of many parallel worker instances under a central coordinator.

> **Authors:** [@Lance_Arthur](https://t.me/Lance_Arthur) · [@naruto0927](https://t.me/naruto0927)

---

## Features

- **Distributed architecture** — multiple workers register with a control group, receive jobs, and report heartbeats independently
- **Auto-rename engine** — parses episode/series metadata and renames files automatically
- **FFmpeg metadata injection** — embeds title, audio, subtitle, and attachment metadata into MKV/MP4 files
- **Reliable download/upload** — retry logic with configurable limits and transfer metrics
- **MediaInfo extraction** — reads codec, resolution, audio, subtitle track info
- **Thumbnail generation** — optional ImgBB upload for file thumbnails
- **Health-check endpoint** — `/health` HTTP route for Render/Heroku keep-alive
- **Graceful shutdown** — waits for in-flight jobs before exiting on SIGINT/SIGTERM

---

## Project Structure

```
worker-bot-main/
├── worker_bot.py          # Entry point — startup, signal handling
├── config.py              # All config loaded from environment variables
├── messages.py            # Bot message templates
├── route.py               # aiohttp health-check route
├── build.sh               # Heroku: downloads static FFmpeg binary at dyno start
├── Dockerfile             # Docker: installs FFmpeg via apt (Render/Docker mode)
├── Procfile               # Heroku process definition
├── render.yaml            # Render deployment configuration
├── requirements.txt       # Python dependencies
├── runtime.txt            # Python 3.10.15
├── helper/
│   ├── ffmpeg.py          # Async FFmpeg/ffprobe wrapper
│   ├── pipeline.py        # Job processing pipeline
│   ├── protocol_handler.py# Worker registration, heartbeat, job intake
│   ├── queue_manager.py   # Concurrent job queue
│   ├── reliable_download.py # Chunked download with retries
│   ├── transfer_metrics.py# Download/upload speed tracking
│   ├── upload_manager.py  # Telegram upload with retries
│   ├── userbot.py         # Pyrogram userbot helpers
│   └── utils.py           # Shared utilities
├── plugins/
│   ├── auto_rename_engine.py  # Episode/series name parser & renamer
│   ├── global_metadata.py     # Metadata templates
│   └── mediainfo.py           # Media track info extraction
├── shared/
│   └── protocol.py        # Shared message protocol (worker ↔ coordinator)
└── tests/
    ├── test_episode_parser.py
    └── test_metadata_pipeline.py
```

---

## Requirements

- Python 3.10.15
- FFmpeg + mkvtoolnix (installed via apt in Docker, or downloaded at startup on Heroku)
- MongoDB (Atlas or any URI)
- Telegram API credentials

### Python Dependencies

```
pyrogram==2.0.106
TgCrypto
motor
dnspython
Pillow
hachoir
mutagen
aiohttp
pytz
psutil
```

---

## Environment Variables

Copy `.env.example` to `.env` for local development. **Never commit `.env`.**

### Required

| Variable | Description |
|---|---|
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API hash |
| `BOT_TOKEN` | This worker's unique bot token |
| `WORKER_CONTROL_GROUP_ID` | Telegram group ID for job dispatch |
| `WORKER_OUTPUT_CHANNEL_ID` | Telegram channel ID for uploads |
| `MONGO_URI` | MongoDB connection URI |
| `DB_NAME` | Database name (default: `DistributedRenameBot`) |

### Optional / Tunable

| Variable | Default | Description |
|---|---|---|
| `WORKER_ID` | `worker_01` | Unique ID across all workers |
| `WORKER_CONCURRENCY` | `3` | Max concurrent jobs (1–10) |
| `WORKER_VERSION` | `1.0.0` | Version string |
| `HEARTBEAT_INTERVAL` | `30` | Seconds between heartbeats (15–300) |
| `WORKER_OFFLINE_TIMEOUT` | `120` | Seconds before coordinator marks worker offline |
| `SHUTDOWN_GRACE_SECONDS` | `60` | Seconds to wait for in-flight jobs on shutdown |
| `PORT` | `8015` | Health-check HTTP port |
| `IMGBB_API_KEY` | *(empty)* | ImgBB key for thumbnail upload (optional) |
| `MAX_DOWNLOAD_RETRIES` | `4` | Download retry attempts |
| `MAX_UPLOAD_RETRIES` | `4` | Upload retry attempts |
| `FFMPEG_TIMEOUT` | `600` | FFmpeg subprocess timeout (seconds) |
| `HTTP_TIMEOUT` | `30` | HTTP request timeout (seconds) |
| `MAX_QUEUE_SIZE` | `50` | Max queued jobs |

---

## Deployment

### Docker / Render (Recommended)

Render uses the `Dockerfile` directly — FFmpeg is installed via `apt`.

1. Push this repo to GitHub.
2. Create a new **Web Service** on [Render](https://render.com), selecting **Docker** as the environment.
3. Set all required environment variables in the Render dashboard (**never** in `render.yaml`).
4. Deploy. Render will build the Docker image and start the service.

The health-check endpoint at `/health` keeps the service alive.

### Heroku

Heroku uses `Procfile` + `build.sh`. The release phase (`build.sh`) downloads static FFmpeg and mkvpropedit binaries because Heroku's filesystem is ephemeral.

```bash
heroku create your-app-name
heroku config:set API_ID=... API_HASH=... BOT_TOKEN=... \
  WORKER_CONTROL_GROUP_ID=... WORKER_OUTPUT_CHANNEL_ID=... \
  MONGO_URI=... DB_NAME=DistributedRenameBot
git push heroku main
```

### Local Development

```bash
git clone <repo>
cd worker-bot-main
cp .env.example .env
# Fill in .env values
pip install -r requirements.txt
python worker_bot.py
```

---

## Startup Sequence

1. Validate all required config — exits cleanly with a helpful error if anything is missing
2. Bootstrap FFmpeg (download static binary on Heroku, or verify apt install in Docker)
3. Bootstrap mkvpropedit (non-fatal if unavailable)
4. Connect Worker Bot via Pyrogram using `BOT_TOKEN`
5. Instantiate `JobPipeline` and `WorkerProtocolHandler`
6. Send `REGISTER` message to control group and start heartbeat loop
7. Start aiohttp health-check server
8. Block until `SIGINT`/`SIGTERM`

---

## Running Tests

```bash
pytest tests/
```

Tests cover the episode name parser and the metadata pipeline.

---

## License

See `Copyright.txt`. Project by [@naruto0927](https://t.me/naruto0927).
