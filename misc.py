import threading
import time
import requests

# Shared state for global cooldowns
_rate_lock = threading.Lock()
_last_request_time = 0  # Timestamp of the last successful request completion
_sleep_until = 0  # For rate limit backoff

# Minimum cooldown between requests (0.4 seconds)
COOLDOWN = 0.4
RATE_LIMIT_SLEEP = 2

# Save the original Session request method
original_session_request = requests.Session.request


def patched_session_request(self, *args, **kwargs):
    global _last_request_time, _sleep_until

    while True:  # Loop for retries
        # Check for ongoing rate limit sleep
        with _rate_lock:
            now = time.time()
            if _sleep_until > now:
                sleep_duration = _sleep_until - now
                time.sleep(sleep_duration)

        # Enforce global cooldown: wait if last request was too recent
        with _rate_lock:
            now = time.time()
            if now - _last_request_time < COOLDOWN:
                sleep_duration = COOLDOWN - (now - _last_request_time)
                time.sleep(sleep_duration)
            # Update to current time temporarily; will finalize after success
            temp_last_time = time.time()

        # Make the request
        response = original_session_request(self, *args, **kwargs)

        # Check for rate limit
        try:
            data = response.json()
            if isinstance(data, dict) and data.get('message') == 'API rate limit exceeded':
                with _rate_lock:
                    _sleep_until = time.time() + RATE_LIMIT_SLEEP
                continue  # Retry after sleep (handled in next iteration)
        except ValueError:
            pass  # Not JSON, skip check

        # If successful (no rate limit), finalize last request time
        with _rate_lock:
            _last_request_time = temp_last_time

        return response