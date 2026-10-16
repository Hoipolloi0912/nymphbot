import io
import random
import asyncio
import aiohttp
import discord
import subprocess

class AudioManager:
    def __init__(self, voice, duration, header, on_finished):
        self.voice = voice
        self.duration = duration
        self.header = header
        self.on_finished = on_finished
        self.urls = asyncio.Queue(maxsize=2)
        self.ready = asyncio.Queue(maxsize=2)
        self.worker = None
        self.session = None

    async def start(self):
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        self.worker = asyncio.create_task(self._worker())

    def toggle_pause(self):
        if not self.voice or not self.voice.is_connected():
            return False

        if self.voice.is_paused():
            self.voice.resume()
        elif self.voice.is_playing():
            self.voice.pause()

    def enqueue(self, url):
        self.urls.put_nowait(url)

    async def run_ffmpeg(self, cmd, data):
        def run():
            result = subprocess.run(cmd, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 0: raise RuntimeError(result.stderr.decode(errors="replace"))
            return result.stdout
        return await asyncio.to_thread(run)

    async def _worker(self):
        while True:
            url = await self.urls.get()
            if not url:
                print("Skipping audio source: missing link")
                await self.ready.put(None)
                continue

            try:
                audio = None

                for attempt in range(3):
                    try:
                        data = await self.fetch(url)
                        duration = await self.probe(data)
                        start = random.uniform(0, max(0, duration - self.duration))
                        audio = await self.prepare(data, start)
                        break

                    except asyncio.CancelledError:
                        raise

                    except Exception as e:
                        print(f"audio source failed ({attempt + 1}/3) for {url}: {e}")
                        if attempt < 2:
                            await asyncio.sleep(1)

                await self.ready.put(audio)

            except asyncio.CancelledError:
                if audio is not None:
                    audio.close()
                raise

            except Exception as e:
                print(f"worker error on {url}: {e}")
                if audio is not None:
                    audio.close()
                await self.ready.put(None)

            finally:
                self.urls.task_done()

    async def fetch(self, url):
        async with self.session.get(self.header + url) as resp:
            resp.raise_for_status()
            return await resp.read()

    async def probe(self, data):
        stdout = await self.run_ffmpeg(["ffmpeg", "-v", "error", "-i", "pipe:0", "-map",
                                        "0:a:0", "-f", "null", "-", "-progress", "pipe:1"], data)
        times = [int(line.split("=", 1)[1]) for line in stdout.decode().splitlines()
                 if line.startswith("out_time_us=") and line.split("=", 1)[1].isdigit()]
        if not times: raise RuntimeError("Could not determine audio duration")
        return times[-1] / 1_000_000

    async def prepare(self, data, start):
        stdout = await self.run_ffmpeg(["ffmpeg", "-hide_banner", "-loglevel", "error", "-i", "pipe:0",
                                        "-ss", str(start), "-t", str(self.duration), "-vn", "-c:a",
                                        "libmp3lame", "-q:a", "4", "-f", "mp3", "pipe:1"], data)
        if not stdout: raise RuntimeError("Empty audio")
        return io.BytesIO(stdout)

    async def play_next(self):
        audio = await self.ready.get()
        if audio is None:return False
        source = discord.FFmpegOpusAudio(audio, pipe=True,
                                         options='-vn -af "loudnorm=I=-20:TP=-1.5:LRA=11"')

        loop = asyncio.get_running_loop()

        def after(error):
            if error: print(f"Playback error: {error}")

            if self.on_finished:
                future = asyncio.run_coroutine_threadsafe(self.on_finished(), loop)

                def log_error(future):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"failed to advance round: {e}")

                future.add_done_callback(log_error)

        try:
            self.voice.play(source, after=after)
        except Exception:
            source.cleanup()
            audio.close()
            raise

        return True

    async def close(self):
        self.voice.stop()

        if self.worker:
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
            self.worker = None

        if self.session:
            await self.session.close()
            self.session = None

        self.urls = None
        self.ready = None