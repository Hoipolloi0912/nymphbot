import io
import random
import asyncio
import aiohttp
import discord

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

    async def _worker(self):
        while True:
            url = await self.urls.get()

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

                if audio is None:print(f"skipping audio: {url}")

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
        proc = await asyncio.create_subprocess_exec("ffprobe", "-v", "error", "-show_entries", "format=duration",
                                                    "-of", "default=noprint_wrappers=1:nokey=1", "-i", "pipe:0",
                                                    stdin=asyncio.subprocess.PIPE,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)

        try:
            stdout, stderr = await proc.communicate(data)
        except asyncio.CancelledError:
            proc.kill()
            await proc.communicate()
            raise

        if proc.returncode != 0:
            raise RuntimeError(stderr.decode(errors="replace"))

        duration = float(stdout.decode().strip())

        if not 0 < duration < float("inf"):
            raise ValueError("Invalid audio duration")

        return duration

    async def prepare(self, data, start):
        proc = await asyncio.create_subprocess_exec("ffmpeg", "-hide_banner", "-loglevel", "error",
                                                    "-i", "pipe:0", "-ss", str(start), "-t", str(self.duration),
                                                    "-vn", "-c:a", "libmp3lame", "-q:a", "4", "-f", "mp3", "pipe:1",
                                                    stdin=asyncio.subprocess.PIPE,
                                                    stdout=asyncio.subprocess.PIPE,
                                                    stderr=asyncio.subprocess.PIPE)

        try:
            stdout, stderr = await proc.communicate(data)
        except asyncio.CancelledError:
            proc.kill()
            await proc.communicate()
            raise

        if proc.returncode != 0 or not stdout:
            raise RuntimeError(stderr.decode(errors="replace") or "Empty audio")

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