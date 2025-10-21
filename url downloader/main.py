# main.py
#!/usr/bin/env python3
"""
Production-Ready Download Manager Backend
FastAPI + WebSocket integration with integrated downloader6.py
"""

import asyncio
import json
import logging
import os
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import psutil
import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Fix asyncio on Windows
asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# -----------------------
# INTEGRATED DOWNLOADER6.PY CODE
# -----------------------

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, unquote
import sys
import random
import math
import queue
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
from collections import defaultdict
from datetime import datetime as dt_datetime

# -----------------------
# USER AGENTS FOR ROTATION
# -----------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# -----------------------
# OPTIMIZED CONFIGURATION
# -----------------------
WORKER_CHUNK_READ = 512 * 1024                    # 512 KB (was 64 KB)
INITIAL_TEST_BYTES = 10 * 1024 * 1024             # 10 MB (was 1 MB)
INITIAL_WORKERS = 8                               # Start safe, scale up
MAX_WORKERS = 128                                 # Allow up to 128
MONITOR_INTERVAL = 2                              # Check every 2 seconds (was 6)
SCALE_UP_THRESHOLD_MBPS = 5.0                     # Scale up if <5 MB/s (was 1.0)
SCALE_DOWN_THRESHOLD_MBPS = 100.0                 # Scale down if >100 MB/s (was 20.0)
MAX_RETRIES_PER_TASK = 5
REQUEST_TIMEOUT = 30                              # Longer timeout for slow servers
MEMORY_THRESHOLD_PERCENT = 90                     # Pause if RAM >80%
GRADUAL_SCALE_STEP = 2                            # Add 2 workers at a time
SCALE_CHECK_INTERVAL = 4                          # Check before scaling every 4 MB downloaded
QUIET = False                                     # Global quiet flag
update_queue: asyncio.Queue

# -----------------------
# THREAD SAFETY LOCKS
# -----------------------
# MEMORY MONITORING
# -----------------------
class MemoryMonitor:
    def __init__(self):
        self.current_percent = 0
        self.is_constrained = False
    
    def update(self):
        """Update memory usage and check if constrained"""
        with threading.Lock(): # Use a local lock
            self.current_percent = psutil.virtual_memory().percent
            self.is_constrained = self.current_percent > MEMORY_THRESHOLD_PERCENT
    
    def wait_if_constrained(self):
        """Sleep if memory is constrained"""
        self.update()
        if self.is_constrained:
            print(f"\n⚠️ Memory constrained ({self.current_percent:.1f}%). Throttling downloads...")
            time.sleep(2)

memory_monitor = MemoryMonitor() # This can remain a single global instance

# -----------------------
# DYNAMIC CHUNK SIZE
# -----------------------
def determine_chunk_size(total_size):
    """Intelligent chunk sizing based on file size"""
    gb = total_size / (1024 * 1024 * 1024)
    if gb < 3:
        return 8 * 1024 * 1024              # 8 MB (was 1 MB)
    elif gb < 5:
        return 16 * 1024 * 1024             # 16 MB (was 4 MB)
    elif gb < 10:
        return 32 * 1024 * 1024             # 32 MB (was 8 MB)
    else:
        return 64 * 1024 * 1024             # 64 MB (was 16 MB)

# -----------------------
# MIRROR STATISTICS TRACKING
# -----------------------
class MirrorStats:
    def __init__(self):
        self.stats = defaultdict(lambda: {
            'success': 0,
            'failure': 0,
            'total_bytes': 0,
            'last_used': 0,
            'latency': float('inf'),
            'throughput_mbps': 0.0,
            'circuit_open': False,
            'circuit_fail_count': 0
        })
    
    def record_success(self, url, bytes_transferred, latency):
        with threading.Lock():
            self.stats[url]['success'] += 1
            self.stats[url]['total_bytes'] += bytes_transferred
            self.stats[url]['last_used'] = time.time()
            self.stats[url]['latency'] = latency
            self.stats[url]['circuit_fail_count'] = max(0, self.stats[url]['circuit_fail_count'] - 1)
    
    def record_failure(self, url):
        with threading.Lock():
            self.stats[url]['failure'] += 1
            self.stats[url]['circuit_fail_count'] += 1
            
            # Circuit breaker: open after 5 consecutive failures
            if self.stats[url]['circuit_fail_count'] >= 5:
                self.stats[url]['circuit_open'] = True
    
    def is_circuit_open(self, url):
        """Check if mirror should be blacklisted"""
        with threading.Lock():
            if self.stats[url]['circuit_open']:
                # Try reopening after 30 seconds
                if time.time() - self.stats[url]['last_used'] > 30:
                    self.stats[url]['circuit_open'] = False
                    self.stats[url]['circuit_fail_count'] = 0
                    return False
                return True
            return False
    
    def get_health(self, url):
        """Get failure rate for a mirror"""
        with threading.Lock():
            total = self.stats[url]['success'] + self.stats[url]['failure']
            if total == 0:
                return 0.0
            return self.stats[url]['failure'] / total
# NETWORKING HELPERS
# -----------------------
def create_session(use_http2=False):
    """Create optimized session with better connection pooling"""
    session = requests.Session()
    
    retries = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True
    )
    
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=128,              # Up from 10
        pool_maxsize=128,                  # Up from 10
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Keep-Alive headers
    session.headers.update({
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Keep-Alive': 'timeout=60, max=1000',
        'User-Agent': random.choice(USER_AGENTS)
    })
    
    return session

def rotate_user_agent(session):
    """Rotate user agent to avoid ISP/server detection"""
    session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    return session

def is_valid_url(url: str) -> bool:
    """Validate URL format"""
    try:
        p = urlparse(url)
        return all([p.scheme, p.netloc])
    except:
        return False

# -----------------------
# FILENAME EXTRACTION
# -----------------------
def extract_filename_from_response(url, session):
    """Extract and sanitize filename from headers or URL."""
    try:
        r = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        
        cd = r.headers.get("Content-Disposition")
        if cd:
            # More robust parsing for Content-Disposition
            # Handles cases like: filename="..."; filename*=UTF-8''...
            fname_match = re.search(r'filename\*?=(.+)', cd)
            if fname_match:
                fname_raw = fname_match.group(1)
                # Handle UTF-8 encoding prefix
                if fname_raw.lower().startswith("utf-8''"):
                    fname_raw = fname_raw[7:]
                
                # Clean up quotes and decode
                fname = unquote(fname_raw.strip(' "\''))
                
                # Sanitize filename for Windows
                fname = re.sub(r'[<>:"/\\|?*]', '_', fname)
                return fname.split(';')[0] # Take only the first part before any semicolons
        
        path = urlparse(r.url).path # Fallback to URL path
        if path:
            return unquote(path.rsplit("/", 1)[-1])
    except:
        pass
    
    path = urlparse(url).path
    return unquote(path.rsplit("/", 1)[-1] or "downloaded.file")

# -----------------------
# AUTO-MIRROR DETECTION
# -----------------------
def detect_mirrors(url, session):
    """Detect mirrors through redirect chains"""
    mirrors = []
    try:
        r = session.get(url, allow_redirects=True, stream=True, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        
        # Collect all URLs in redirect chain
        redirects = [r.url]
        if r.history:
            for resp in r.history:
                if 'Location' in resp.headers:
                    redirects.append(resp.headers['Location'])
        
        mirrors = list(dict.fromkeys(redirects))  # Remove duplicates
    except:
        mirrors = [url]
    
    return mirrors if mirrors else [url]

# -----------------------
# MIRROR PROBING & RANKING
# -----------------------
def probe_mirror(url, session, test_bytes=INITIAL_TEST_BYTES):
    """Probe mirror quality"""
    result = {
        'url': url,
        'ok': False,
        'accept_ranges': False,
        'content_length': 0,
        'latency': float('inf'),
        'throughput_mbps': 0.0,
        'headers': {}
    }
    
    try:
        t0 = time.time()
        head = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
        latency = time.time() - t0
        head.raise_for_status()
        
        result['ok'] = True
        result['latency'] = latency
        result['headers'] = dict(head.headers)
        
        cl = head.headers.get('content-length')
        content_length = int(cl) if cl and cl.isdigit() else 0

        # Fallback: If HEAD request doesn't give content-length, try a streaming GET
        # with a Range request, which can sometimes force a 'Content-Range' header.
        if content_length == 0:
            get_req = session.get(url, headers={'Range': 'bytes=0-0'}, allow_redirects=True, stream=True, timeout=REQUEST_TIMEOUT)
            # Don't raise for status here, just check for the header
            cr_get = get_req.headers.get('content-range') # e.g., "bytes 0-0/209715200"
            if cr_get and '/' in cr_get:
                content_length = int(cr_get.split('/')[-1])
            else: # If even that fails, try one last time with a normal GET's content-length
                cl_get = get_req.headers.get('content-length')
                content_length = int(cl_get) if cl_get and cl_get.isdigit() else 0

        result['content_length'] = content_length
        result['accept_ranges'] = head.headers.get('Accept-Ranges', '').lower() == 'bytes'
        
        # Test throughput if range requests supported
        if result['accept_ranges'] and content_length > 0:
            headers = {'Range': f'bytes=0-{max(test_bytes-1, 0)}'}
            t_start = time.time()
            r = session.get(url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            
            downloaded = 0
            for chunk in r.iter_content(chunk_size=WORKER_CHUNK_READ):
                if not chunk:
                    break
                downloaded += len(chunk)
                if downloaded >= test_bytes:
                    break
            
            elapsed = max(time.time() - t_start, 1e-6)
            result['throughput_mbps'] = (downloaded / (1024 * 1024)) / elapsed
        else:
            # Fallback: test without range
            t_start = time.time()
            r = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            
            downloaded = 0
            for chunk in r.iter_content(chunk_size=WORKER_CHUNK_READ):
                if not chunk:
                    break
                downloaded += len(chunk)
                if downloaded >= 32 * 1024:
                    break
            
            elapsed = max(time.time() - t_start, 1e-6)
            result['throughput_mbps'] = (downloaded / (1024 * 1024)) / elapsed
    
    except Exception as e:
        pass
    
    return result

def rank_mirrors(urls, session):
    """Rank mirrors by quality (parallel testing)"""
    probes = []
    
    # Parallel mirror testing
    with ThreadPoolExecutor(max_workers=min(len(urls), 4)) as executor:
        futures = {executor.submit(probe_mirror, u, session): u for u in urls}
        for future in as_completed(futures):
            try:
                probes.append(future.result())
            except:
                pass
    
    # Filter alive mirrors
    alive = [p for p in probes if p['ok']]
    
    if not alive:
        return []
    
    # Sort by throughput (descending), then latency (ascending)
    alive.sort(key=lambda x: (-x['throughput_mbps'], x['latency']))
    
    return alive

# -----------------------
# TASK QUEUE MANAGEMENT
# -----------------------
def create_range_tasks(total_size, start_offset=0, chunk_size=1*1024*1024):
    """Create download range tasks"""
    q = queue.Queue()
    
    if start_offset >= total_size:
        return q
    
    pos = start_offset
    while pos < total_size:
        end = min(pos + chunk_size - 1, total_size - 1)
        q.put((pos, end))
        pos = end + 1
    
    return q

def weighted_choice(mirrors):
    """Select mirror using weighted randomization"""
    total = sum(m['weight'] for m in mirrors)
    
    if total <= 0:
        return random.choice(mirrors)['url']
    
    r = random.uniform(0, total)
    upto = 0
    
    for m in mirrors:
        upto += m['weight']
        if upto >= r:
            return m['url']
    
    return mirrors[-1]['url']

# -----------------------
# WORKER THREAD
# -----------------------
def worker_thread(job):
    """Worker thread for downloading chunks."""
    session = create_session()
    
    # Unpack job context
    stop_event, task_queue, mirrors, filepath, progress, progress_callback, mirror_stats = job.stop_event, job.task_queue, job.mirrors, job.filepath, job.progress, job.progress_callback, job.mirror_stats
    while not stop_event.is_set():
        try:
            start, end = task_queue.get_nowait()
        except queue.Empty:
            return

        success = False
        # The pause event is part of the job context now
        pause_event = job.pause_event
        pause_event.wait() # Check if paused before starting a new task
        attempt = 0

        while not success and attempt < MAX_RETRIES_PER_TASK and not stop_event.is_set():
            attempt += 1

            # Memory check
            if not QUIET:
                memory_monitor.wait_if_constrained()
            else:
                memory_monitor.update()
                if memory_monitor.is_constrained:
                    time.sleep(2)

            # Self-healing: if a mirror is consistently failing, force a re-probe
            if attempt > 2 and mirror_stats.get_health(url) > 0.5:
                if not job.quiet:
                    print(f"⚠️ High failure rate on {url}. Re-probing mirrors...")
                
                # Re-rank and update mirrors for all workers
                new_probes = rank_mirrors([m['url'] for m in mirrors], session)
                if new_probes:
                    total_tp = sum(max(0.0001, p['throughput_mbps']) for p in new_probes)
                    job.mirrors.clear() # Safely update the shared list
                    for p in new_probes:
                        job.mirrors.append({'url': p['url'], 'weight': max(0.0001, p['throughput_mbps']) / total_tp, 'accept_ranges': p['accept_ranges']})
                    if not job.quiet: print("✅ Mirrors re-ranked.")

            url = weighted_choice(mirrors)

            # Skip if circuit open
            if mirror_stats.is_circuit_open(url):
                continue

            headers = {'Range': f'bytes={start}-{end}'}

            try:
                # Rotate user agent occasionally
                if random.random() < 0.1:
                    session = rotate_user_agent(session)

                t_start = time.time()
                r = session.get(url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()

                # Use standard synchronous I/O. The previous aiofiles implementation
                # created isolated event loops, which broke the progress callback.
                with open(filepath, 'r+b') as f:
                    f.seek(start)
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=WORKER_CHUNK_READ):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            with threading.Lock():
                                progress.update(len(chunk))
                                if progress_callback:
                                    progress_callback(len(chunk), progress.n, progress.total)

                if downloaded >= end - start + 1:
                    success = True
                    elapsed = max(time.time() - t_start, 1e-6)
                    mirror_stats.record_success(url, downloaded, elapsed)

            except Exception as e:
                mirror_stats.record_failure(url)
                time.sleep(0.5 * (2 ** (attempt - 1)))  # Exponential backoff
                continue

        if not success:
            try:
                task_queue.put((start, end))
            except:
                pass

        task_queue.task_done()

# -----------------------
# MONITORING & SCALING
# -----------------------
def monitor_and_scale(job):
    """Monitor throughput and scale workers dynamically"""
    # Unpack job context, including the pause_event
    stop_event, task_queue, worker_pool, progress, mirrors, filepath, progress_callback, mirror_stats, pause_event = \
        job.stop_event, job.task_queue, job.worker_pool, job.progress, job.mirrors, job.filepath, job.progress_callback, job.mirror_stats, job.pause_event

    last_bytes = progress.n
    worker_scale_cooldown = time.time()

    while not stop_event.is_set():
        pause_event.wait() # Wait here if the download is paused
        time.sleep(MONITOR_INTERVAL)
        if stop_event.is_set(): break
        
        new_bytes = progress.n
        delta = new_bytes - last_bytes
        last_bytes = new_bytes

        mbps = (delta / (1024 * 1024)) / max(MONITOR_INTERVAL, 1e-6)
        queue_size = task_queue.qsize()

        # Print status only if not quiet
        if not QUIET:
            print(f"\n📊 [Monitor] Speed: {mbps:.2f} MB/s | Queue: {queue_size} tasks | "
                  f"Workers: {len(worker_pool)} | Memory: {memory_monitor.current_percent:.1f}%")

        # Scaling logic with cooldown
        current_time = time.time()
        if current_time - worker_scale_cooldown > 10:  # 10 second cooldown between scales

            # If memory is constrained, DO NOT scale up. This prevents the death spiral.
            if memory_monitor.is_constrained:
                if not QUIET:
                    print(f"🧠 [Monitor] Scaling paused due to high memory usage ({memory_monitor.current_percent:.1f}%).")
                continue # Skip scaling logic for this interval

            # Scale UP
            if 0 < mbps < SCALE_UP_THRESHOLD_MBPS and len(worker_pool) < MAX_WORKERS and queue_size > 4:
                scale_count = min(GRADUAL_SCALE_STEP, MAX_WORKERS - len(worker_pool))
                if not QUIET:
                    print(f"⬆️ Scaling UP by {scale_count} workers (low speed: {mbps:.2f} MB/s)")

                for _ in range(scale_count):
                    t = threading.Thread(
                        target=worker_thread,
                        args=(job,),
                        daemon=True
                    )
                    t.start()
                    worker_pool.append(t)

                worker_scale_cooldown = current_time

            # Scale DOWN
            elif mbps > SCALE_DOWN_THRESHOLD_MBPS and len(worker_pool) > INITIAL_WORKERS and queue_size < 2:
                scale_count = min(GRADUAL_SCALE_STEP, len(worker_pool) - INITIAL_WORKERS)
                if not QUIET:
                    print(f"⬇️ Scaling DOWN by {scale_count} workers (high speed: {mbps:.2f} MB/s)")

                for _ in range(scale_count):
                    if worker_pool:
                        worker_pool.pop()

                worker_scale_cooldown = current_time

    # Final memory check
    memory_monitor.update()
    if not QUIET:
        print(f"\n✅ Download complete. Final memory usage: {memory_monitor.current_percent:.1f}%")

# -----------------------
# DOWNLOAD JOB CONTEXT
# -----------------------
class DownloadJob:
    """Encapsulates all state for a single download job."""
    def __init__(self, filename, output_dir, progress_callback, stop_event, pause_event, quiet=False):
        self.filename = filename
        self.filepath = os.path.join(output_dir, filename)
        self.progress_callback = progress_callback
        self.stop_event = stop_event
        self.pause_event = pause_event
        self.quiet = quiet

        # Job-specific state
        self.mirrors = []
        self.mirror_stats = MirrorStats()
        self.task_queue = queue.Queue()
        self.worker_pool = []
        self.progress = None # tqdm instance

# -----------------------
# MAIN DOWNLOAD FUNCTION
# -----------------------
def multi_mirror_download(urls, filename, output_dir, progress_callback=None, quiet=False, stop_event=None, pause_event=None, resume_offset=0):
    """Main download orchestration"""
    job = DownloadJob(filename, output_dir, progress_callback, stop_event or threading.Event(),
                      pause_event or threading.Event(), quiet)

    session = create_session()

    if not quiet:
        print("\n🔍 Probing mirrors...")
    probe_results = rank_mirrors(urls, session)

    if not probe_results:
        raise RuntimeError("❌ No reachable mirrors found.")

    if not quiet:
        print(f"✅ Found {len(probe_results)} working mirror(s)")
        for i, p in enumerate(probe_results, 1):
            print(f"  {i}. {p['url']} - {p['throughput_mbps']:.2f} MB/s (latency: {p['latency']*1000:.0f}ms)")
        job.mirrors.clear() # Ensure we start with a fresh list

    # Calculate weighted mirrors
    total_tp = sum(max(0.0001, p['throughput_mbps']) for p in probe_results)

    for p in probe_results:
        weight = max(0.0001, p['throughput_mbps']) / total_tp if total_tp > 0 else 1.0 / len(probe_results)
        job.mirrors.append({
            'url': p['url'],
            'weight': weight,
            'accept_ranges': p['accept_ranges']
        })
    
    total_size = probe_results[0]['content_length']

    if total_size == 0:
        raise RuntimeError("❌ Could not determine file size.")

    chunk_size = determine_chunk_size(total_size)

    if not quiet:
        print(f"\n📥 File size: {total_size / (1024*1024*1024):.2f} GB")
        print(f"📦 Chunk size: {chunk_size / (1024*1024):.1f} MB")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Resume logic
    # Use the passed resume_offset, which is more reliable than os.path.getsize for sparse files.
    if resume_offset > 0 and os.path.exists(job.filepath):
        if resume_offset >= total_size:
            if not quiet:
                print("✅ File already downloaded completely!")
            return

        if not quiet:
            print(f"🔄 Resuming from {resume_offset / (1024*1024):.2f} MB")

    # Create task queue
    job.task_queue = create_range_tasks(total_size, start_offset=resume_offset, chunk_size=chunk_size)

    # Pre-allocate file
    if not os.path.exists(job.filepath):
        # Use fast sparse file allocation instead of slow truncate()
        if total_size > 0:
            with open(job.filepath, 'wb') as f:
                f.seek(total_size - 1)
                f.write(b'\0')
    if not quiet:
        print(f"💾 File pre-allocated: {job.filepath}")

    # Progress bar
    job.progress = tqdm(
        total=total_size,
        unit='B',
        unit_scale=True,
        desc=filename,
        dynamic_ncols=True,
        disable=quiet
    )

    if resume_offset:
        job.progress.update(resume_offset)

    # Start workers
    initial_workers = min(INITIAL_WORKERS, MAX_WORKERS, max(1, math.ceil(sum(m['weight'] for m in job.mirrors) * 4)))
    if not quiet:
        print(f"\n🚀 Starting with {initial_workers} workers...")

    for i in range(initial_workers):
        t = threading.Thread(
            target=worker_thread,
            args=(job,),
            daemon=True
        )
        t.start()
        job.worker_pool.append(t)

    # Start monitor
    monitor = threading.Thread(
        target=monitor_and_scale,
        args=(job,),
        daemon=True
    )
    monitor.start()

    # Wait for completion
    try:
        while not job.task_queue.empty():
            if job.stop_event.is_set():
                if not quiet: print("\n⛔ Stop event received. Halting download.")
                break
            time.sleep(1) # Wait for tasks to be processed
    except KeyboardInterrupt:
        if not quiet:
            print("\n⛔ Interrupted by user. Download paused.")
        job.stop_event.set()
        # sys.exit(1) # Removed to prevent killing the entire FastAPI process

    # Only set stop_event if the queue is NOT empty (meaning tasks remain or it was externally stopped)
    # If the queue is empty AND stop_event is NOT set, it means successful completion, so we don't set it.
    is_stopped_by_user = job.stop_event.is_set()
    is_incomplete = not job.task_queue.empty()

    if is_stopped_by_user or is_incomplete:
        job.stop_event.set()

    # Wait for threads
    for t in job.worker_pool:
        t.join(timeout=2)

    monitor.join(timeout=2)
    job.progress.close()

    # Return True for successful completion, False otherwise
    return not (is_stopped_by_user or is_incomplete)

# -----------------------
# END OF INTEGRATED DOWNLOADER6.PY CODE
# -----------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DownloadRequest(BaseModel):
    url: str
    filename: Optional[str] = None
    location: str = "downloads"


class DownloadInfo(BaseModel):
    id: str
    url: str
    filename: str
    location: str
    status: str  # pending, downloading, paused, completed, failed, stopped
    progress: float = 0.0
    speed: float = 0.0
    total_size: int = 0
    downloaded: int = 0
    eta: Optional[int] = None
    created_at: str
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    mirrors: List[str] = []
    last_update_time: float = 0.0
    last_downloaded: int = 0


class DownloadManager:
    def __init__(self):
        self.downloads: Dict[str, DownloadInfo] = {}
        self.threads: Dict[str, threading.Thread] = {}
        self.main_loop = asyncio.get_event_loop()
        self.stops: Dict[str, threading.Event] = {}
        self.pauses: Dict[str, threading.Event] = {}
        self.lock = threading.Lock()  # Lock for thread-safe access to self.downloads
        os.makedirs("downloads", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    def add(self, req: DownloadRequest) -> str:
        with self.lock:
            if not is_valid_url(req.url):
                raise ValueError("Invalid URL")
            dl_id = str(uuid.uuid4())
            if not req.filename:
                try:
                    sess = create_session()
                    req.filename = extract_filename_from_response(req.url, sess)
                except:
                    req.filename = f"download_{int(time.time())}"
            info = DownloadInfo(
                id=dl_id, url=req.url, filename=req.filename, location=req.location,
                status="pending", created_at=datetime.now().isoformat(),
                last_update_time=time.time(), last_downloaded=0
            )
            # Use lock to safely add new download
            self.downloads[dl_id] = info

        stop_evt = threading.Event()
        pause_evt = threading.Event()
        pause_evt.set() # Start in a "running" state
        self.stops[dl_id] = stop_evt
        self.pauses[dl_id] = pause_evt
        thr = threading.Thread(target=self._worker, args=(dl_id,), daemon=True)
        thr.start()
        self.threads[dl_id] = thr
        return dl_id

    def _progress_callback(self, dl_id: str, chunk_size: int, downloaded: int, total: int):
        # Acquire lock to safely update the shared download info object
        with self.lock:
            if dl_id not in self.downloads:
                return

            current_time = time.time()
            info = self.downloads[dl_id] # Access under lock

            time_diff = current_time - info.last_update_time
            if time_diff < 0.01: # Throttle updates to 100 per second max
                return

            bytes_diff = downloaded - info.last_downloaded
            info.downloaded = downloaded
            info.total_size = total
            info.progress = (downloaded / total) * 100 if total > 0 else 0
            info.speed = bytes_diff / time_diff if time_diff > 0 else 0
            info.eta = int((total - downloaded) / info.speed) if info.speed > 0 else None
            info.last_update_time = current_time
            info.last_downloaded = downloaded

        # This part is now outside the lock for performance
        # Create a minimal, focused update object
        metrics_update = {
            "type": "metrics_update",
            "id": dl_id,
            "progress": info.progress,
            "speed": info.speed,
            "eta": info.eta,
            "downloaded": info.downloaded,
        }
        # Use call_soon_threadsafe to put item into asyncio.Queue from a worker thread
        self.main_loop.call_soon_threadsafe(update_queue.put_nowait, metrics_update)

    def _worker(self, dl_id: str):
        try:
            with self.lock:
                info = self.downloads[dl_id]

            logger.info(f"[{dl_id}] Pre-flight checks for URL: {info.url}")
            sess = create_session()
            mirrors = detect_mirrors(info.url, sess)

            if not mirrors:
                raise ValueError("No mirrors detected")
            
            # This pre-flight check is crucial for getting total_size before starting
            probe_results = rank_mirrors(mirrors, sess)
            if not probe_results:
                raise ValueError("No reachable mirrors found.")
            
            total_size = probe_results[0]['content_length']
            if total_size <= 0:
                raise ValueError("Could not determine file size.")

            # Safely update the DownloadInfo object with pre-flight data
            with self.lock:
                info.status = "downloading"
                info.mirrors = [p['url'] for p in probe_results]
                info.total_size = total_size
            
            logger.info(f"[{dl_id}] Starting multi_mirror_download. Size: {total_size} bytes.")

            # Use the advanced multi_mirror_download function with progress callback
            def progress_cb(chunk_size, downloaded, total):
                self._progress_callback(dl_id, chunk_size, downloaded, total)

            resume_offset = info.downloaded

            was_successful = multi_mirror_download(
                mirrors, info.filename, info.location,
                progress_callback=progress_cb,
                quiet=False,
                stop_event=self.stops[dl_id],
                pause_event=self.pauses[dl_id],
                resume_offset=resume_offset
            )

            if was_successful:
                with self.lock:
                    info.status = "completed"
                    info.progress = 100.0
                    info.speed = 0.0
                    info.eta = 0
                    info.completed_at = datetime.now().isoformat()
                logger.info(f"Download {dl_id} completed successfully")
        except Exception as e:
            logger.error(f"Download {dl_id} failed in worker: {e}")
            with self.lock:
                if dl_id in self.downloads:
                    self.downloads[dl_id].status = "failed"
                    self.downloads[dl_id].error_message = str(e)

    def _get_events(self, dl_id: str):
        """Helper to get stop and pause events for a download."""
        with self.lock:
            stop_event = self.stops.get(dl_id)
            pause_event = self.pauses.get(dl_id)
            return stop_event, pause_event
    def stop(self, dl_id: str) -> bool:
        if dl_id in self.stops:
            with self.lock:
                if dl_id in self.downloads:
                    self.downloads[dl_id].status = "stopped"
            self.stops[dl_id].set()
            logger.info(f"Download {dl_id} stopped by user")
            return True
        return False

    def pause(self, dl_id: str) -> bool:
        with self.lock:
            if dl_id in self.downloads and self.downloads[dl_id].status == "downloading":
                if dl_id in self.pauses:
                    self.pauses[dl_id].clear() # Clear event to pause workers
                self.downloads[dl_id].status = "paused"
                logger.info(f"Download {dl_id} paused")
                return True
        return False

    def resume(self, dl_id: str) -> bool:
        # No new thread needed, just signal the existing one.
        with self.lock:
            if dl_id in self.downloads and self.downloads[dl_id].status == "paused":
                if dl_id in self.pauses:
                    self.pauses[dl_id].set() # Set event to resume workers
                    self.downloads[dl_id].status = "downloading"
                    logger.info(f"Download {dl_id} resumed")
                    return True
        return False

    def retry(self, dl_id: str) -> bool:
        """Retry a failed or stopped download."""
        with self.lock:
            if dl_id not in self.downloads:
                return False

            info = self.downloads[dl_id]
            if info.status not in ["failed", "stopped", "completed"]:
                return False

            # We no longer delete the file. Instead, we will trust `info.downloaded`
            # to resume correctly from the last known position.

            # Reset download state
            info.status = "pending"
            # Keep info.progress, info.downloaded, and info.last_downloaded
            info.speed = 0.0
            info.eta = None
            info.completed_at = None
            info.error_message = None
            info.last_update_time = time.time()
            # info.last_downloaded is preserved

            # Re-create events and start a new worker thread
            self.stops[dl_id] = threading.Event()
            self.pauses[dl_id] = threading.Event()
            self.pauses[dl_id].set()
            thr = threading.Thread(target=self._worker, args=(dl_id,), daemon=True)
            thr.start()
            self.threads[dl_id] = thr
            logger.info(f"Retrying download {dl_id}")
            return True

    def list(self) -> List[DownloadInfo]:
        with self.lock:
            # Return a copy to prevent modification during iteration
            return [d.copy(deep=True) for d in self.downloads.values()]


class ConnectionManager:
    def __init__(self):
        self.conns: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.conns.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.conns:
            self.conns.remove(ws)

    async def broadcast(self, msg: dict):
        for ws in self.conns[:]:
            try:
                await ws.send_text(json.dumps(msg))
            except Exception:
                self.disconnect(ws)

    async def broadcast_status(self):
        """Broadcast current status to all connected clients"""
        status = await api_status()
        downloads = dm.list()
        msg = {
            "type": "status_update",
            "timestamp": datetime.now().isoformat(),
            "system": status["system"],
            "downloads_summary": status["downloads"],
            "downloads": [d.dict() for d in downloads]
        }
        await self.broadcast(msg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global dm, cm, update_queue
    dm = DownloadManager()
    update_queue = asyncio.Queue()
    cm = ConnectionManager()
    asyncio.create_task(broadcaster())
    logger.info("Application startup complete")
    yield
    # Shutdown
    logger.info("Application shutdown initiated")
    for d in dm.list():
        dm.stop(d.id)
        logger.info(f"Stopped download {d.id} during shutdown")
    logger.info("Application shutdown complete")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.post("/api/add")
async def api_add(req: DownloadRequest):
    try:
        dl_id = dm.add(req)
        return {"success": True, "download_id": dl_id}
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/api/downloads")
async def api_list():
    return {"downloads": [d.dict() for d in dm.list()]}


@app.post("/api/stop/{dl_id}")
async def api_stop(dl_id: str):
    if not dm.stop(dl_id):
        raise HTTPException(404, "Not found")
    return {"success": True}


@app.post("/api/pause/{dl_id}")
async def api_pause(dl_id: str):
    if not dm.pause(dl_id):
        raise HTTPException(400, "Cannot pause")
    return {"success": True}


@app.post("/api/resume/{dl_id}")
async def api_resume(dl_id: str):
    if not dm.resume(dl_id):
        raise HTTPException(400, "Cannot resume")
    return {"success": True}


@app.post("/api/retry/{dl_id}")
async def api_retry(dl_id: str):
    if not dm.retry(dl_id):
        raise HTTPException(400, "Cannot retry this download")
    return {"success": True}


@app.get("/api/status")
async def api_status():
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    cpu = psutil.cpu_percent(interval=0.1)
    dls = dm.list()
    active = [d for d in dls if d.status == "downloading"]
    total_speed = sum(d.speed for d in active)
    return {
        "system": {
            "memory_percent": mem.percent,
            "disk_free_gb": disk.free/(1024**3),
            "cpu_percent": cpu,
            "disk_total_gb": disk.total / (1024**3),
            "disk_used_percent": disk.percent,
        },
        "downloads": {
            "total": len(dls),
            "active": len(active),
            "total_speed": total_speed
        }
    }


@app.get("/api/logs")
async def api_logs():
    path = "logs/app.log"
    if os.path.exists(path):
        with open(path, 'r') as f:
            lines = f.read().splitlines()[-100:]
        return {"logs": lines}
    return {"logs": ["No logs"]}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await cm.connect(ws)
    await cm.broadcast_status()
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect as e:
        logger.info(f"WebSocket disconnected: client {ws.client.host}:{ws.client.port}, reason: {e.code}")
    finally:
        cm.disconnect(ws)

async def metrics_broadcaster():
    """High-frequency broadcaster for individual download metrics."""
    while True:
        try:
            metrics = await update_queue.get()
            await cm.broadcast(metrics)
            update_queue.task_done()
        except Exception as e:
            logger.error(f"Broadcaster metrics error: {e}")

async def status_broadcaster():
    """Low-frequency broadcaster for full system and download list status."""
    while True:
        try:
            await cm.broadcast_status()
        except Exception as e:
            logger.error(f"Status broadcaster error: {e}")
        await asyncio.sleep(1)

async def broadcaster():
    """Runs high-frequency and low-frequency broadcasters concurrently."""
    await asyncio.gather(
        metrics_broadcaster(),
        status_broadcaster()
    )


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, log_level="info")
