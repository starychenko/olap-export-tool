import itertools
import sys
import threading
import time

from .utils import format_time, get_current_time
from colorama import Fore
from typing import Callable


animation_running = False

# Значення за замовчуванням — перевизначаються через init_display()
_ascii_mode = False
_debug = False
_query_timeout = 30
_progress_update_interval_ms = 100

SPINNER_FRAMES = [
    "⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷",
]
COUNTDOWN_ICON = "⏱️"


def init_display(
    ascii_logs: bool = False,
    debug: bool = False,
    query_timeout: int = 30,
    progress_update_interval_ms: int = 100,
) -> None:
    """Ініціалізація модуля після побудови конфігурації."""
    global _ascii_mode, _debug, _query_timeout, _progress_update_interval_ms
    global SPINNER_FRAMES, COUNTDOWN_ICON
    _ascii_mode = ascii_logs
    _debug = debug
    _query_timeout = query_timeout
    _progress_update_interval_ms = max(50, min(500, progress_update_interval_ms))
    if _ascii_mode:
        SPINNER_FRAMES = ["-", "\\", "|", "/"]
        COUNTDOWN_ICON = "*"
    else:
        SPINNER_FRAMES = ["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]
        COUNTDOWN_ICON = "⏱️"


class TimeTracker:
    def __init__(self, total_items: int, query_timeout: int | None = None, debug: bool | None = None):
        self.total_items = total_items
        self.processed_items = 0
        self.start_time = time.time()
        self.elapsed_times: list[float] = []
        self.waiting_times: list[float] = []
        self.last_item_end_time = self.start_time
        self.currently_waiting = False
        self._query_timeout = query_timeout if query_timeout is not None else _query_timeout
        self._debug = debug if debug is not None else _debug

    def start_waiting(self):
        self.currently_waiting = True
        self.wait_start_time = time.time()

    def end_waiting(self):
        if self.currently_waiting:
            self.waiting_times.append(time.time() - self.wait_start_time)
            self.currently_waiting = False

    def update(self, items_processed: int = 1):
        current_time = time.time()
        if self.currently_waiting:
            self.end_waiting()
        if self.processed_items == 0:
            processing_time = current_time - self.start_time
        else:
            processing_time = current_time - self.last_item_end_time
            if self.waiting_times:
                processing_time -= self.waiting_times[-1]
        self.elapsed_times.append(processing_time)
        self.last_item_end_time = current_time
        self.processed_items += items_processed

    def get_elapsed_time(self):
        return time.time() - self.start_time

    def get_processing_time(self):
        return sum(self.elapsed_times) if self.elapsed_times else 0

    def get_waiting_time(self):
        return sum(self.waiting_times) if self.waiting_times else 0

    def get_remaining_processing_time(self):
        if not self.elapsed_times or self.processed_items == 0:
            return None
        num_items_to_use = min(5, len(self.elapsed_times))
        recent_times = self.elapsed_times[-num_items_to_use:]
        avg_time_per_item = sum(recent_times) / len(recent_times)
        if len(self.elapsed_times) < 5 or self.processed_items < self.total_items * 0.1:
            if len(self.elapsed_times) == 1:
                safety_factor = 1.2
            elif len(self.elapsed_times) < 3:
                safety_factor = 1.1
            else:
                safety_factor = 1.05
            avg_time_per_item *= safety_factor
        remaining_items = self.total_items - self.processed_items
        return avg_time_per_item * remaining_items

    def get_remaining_wait_time(self):
        remaining_items = max(0, self.total_items - self.processed_items - 1)
        return self._query_timeout * remaining_items

    def get_remaining_time(self):
        processing_time = self.get_remaining_processing_time()
        if processing_time is None:
            return None
        return processing_time + self.get_remaining_wait_time()

    def get_percentage_complete(self):
        return (
            (self.processed_items / self.total_items) * 100
            if self.total_items > 0
            else 0
        )

    def get_total_time(self):
        remaining = self.get_remaining_time()
        return (
            self.get_elapsed_time()
            if remaining is None
            else self.get_elapsed_time() + remaining
        )

    def get_progress_info(self):
        elapsed = self.get_elapsed_time()
        remaining_total = self.get_remaining_time()
        total = self.get_total_time()
        percentage = self.get_percentage_complete()

        info = (
            f"Прогрес: {percentage:.1f}% ({self.processed_items}/{self.total_items})\n"
        )
        info += f"Минуло: {format_time(elapsed)}"
        if remaining_total is not None:
            accuracy_note = ""
            if len(self.elapsed_times) == 1:
                accuracy_note = " (дуже приблизно)"
            elif len(self.elapsed_times) < 3:
                accuracy_note = " (орієнтовно)"
            info += f" | Залишилось: {format_time(remaining_total)}{accuracy_note} | Всього: {format_time(total)}{accuracy_note}"
        return info


def loading_spinner(description: str, estimated_time: float | None = None):
    global animation_running
    animation_running = True
    spinner = itertools.cycle(SPINNER_FRAMES)
    start_time = time.time()
    message = ""
    while animation_running:
        elapsed = time.time() - start_time
        elapsed_str = format_time(elapsed)
        message = f"{Fore.BLUE}[{get_current_time()}] {next(spinner)} {description} | Час: {elapsed_str}"
        sys.stdout.write("\r" + " " * (len(message) + 2) + "\r")
        sys.stdout.write(message)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write("\r" + " " * (len(message) + 2) + "\r")
    sys.stdout.flush()


def streaming_spinner(
    description: str, stop_event: threading.Event, rows_fn: Callable[[], int],
    update_interval_ms: int | None = None,
):
    """Анімація для стрімінгових експортів з відображенням кількості рядків і часу."""
    spinner = itertools.cycle(SPINNER_FRAMES)
    start_time = time.time()
    last_message = ""
    interval_ms = update_interval_ms if update_interval_ms is not None else _progress_update_interval_ms
    interval_ms = max(50, min(500, interval_ms))
    interval_s = interval_ms / 1000.0
    while not stop_event.is_set():
        elapsed_str = format_time(time.time() - start_time)
        try:
            rows = rows_fn()
        except Exception:
            rows = 0
        message = f"{Fore.BLUE}[{get_current_time()}] {next(spinner)} {description} | Рядків: {rows} | Час: {elapsed_str}"
        sys.stdout.write("\r" + " " * (len(last_message) + 2) + "\r")
        sys.stdout.write(message)
        sys.stdout.flush()
        last_message = message
        time.sleep(interval_s)
    sys.stdout.write("\r" + " " * (len(last_message) + 2) + "\r")
    sys.stdout.flush()


def countdown_timer(seconds: int):
    for remaining in range(seconds, 0, -1):
        time_left = format_time(remaining)
        sys.stdout.write(
            f"\r{Fore.YELLOW}[{get_current_time()}] {COUNTDOWN_ICON}  Очікування: залишилось {time_left}..."
        )
        sys.stdout.flush()
        time.sleep(1)
    print()
