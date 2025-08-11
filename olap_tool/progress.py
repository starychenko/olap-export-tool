import itertools
import os
import sys
import threading
import time

from .utils import format_time, get_current_time
from colorama import Fore
from typing import Callable


animation_running = False


class TimeTracker:
    def __init__(self, total_items: int):
        self.total_items = total_items
        self.processed_items = 0
        self.start_time = time.time()
        self.elapsed_times: list[float] = []
        self.waiting_times: list[float] = []
        self.last_item_end_time = self.start_time
        self.currently_waiting = False

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
        wait_time_per_item = int(os.getenv("QUERY_TIMEOUT", 30))
        remaining_items = max(0, self.total_items - self.processed_items - 1)
        return wait_time_per_item * remaining_items

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
        processing_time = self.get_processing_time()
        waiting_time = self.get_waiting_time()
        remaining_processing = self.get_remaining_processing_time()
        remaining_waiting = self.get_remaining_wait_time()
        remaining_total = self.get_remaining_time()
        total = self.get_total_time()
        percentage = self.get_percentage_complete()

        debug_output = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")
        if debug_output and self.elapsed_times and self.processed_items > 0:
            pass  # скорочено: діагностичні друки залишені в оригіналі

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
    spinner = itertools.cycle(["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"])
    start_time = time.time()
    message = ""
    while animation_running:
        elapsed = time.time() - start_time
        elapsed_str = format_time(elapsed)
        message = f"{Fore.BLUE}[{get_current_time()}] {next(spinner)} {description} | Час: {elapsed_str}"
        # Пишемо лише у старті рядка і очищуємо поточну лінію
        sys.stdout.write("\r" + " " * (len(message) + 2) + "\r")
        sys.stdout.write(message)
        sys.stdout.flush()
        time.sleep(0.1)
    # Очищення рядка спінера
    sys.stdout.write("\r" + " " * (len(message) + 2) + "\r")
    sys.stdout.flush()
    # Додатковий перенос рядка не друкуємо, щоб не ламати наступні логи


def streaming_spinner(
    description: str, stop_event: threading.Event, rows_fn: Callable[[], int]
):
    """Анімація для стрімінгових експортів з відображенням кількості рядків і часу."""
    spinner = itertools.cycle(["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"])
    start_time = time.time()
    last_message = ""
    # Інтервал оновлення, мс (за замовчуванням 100 мс). Діапазон: 50..500 мс
    try:
        interval_ms = int(os.getenv("PROGRESS_UPDATE_INTERVAL_MS", "100"))
    except Exception:
        interval_ms = 100
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
    # Очищення рядка після завершення
    sys.stdout.write("\r" + " " * (len(last_message) + 2) + "\r")
    sys.stdout.flush()


def countdown_timer(seconds: int):
    for remaining in range(seconds, 0, -1):
        time_left = format_time(remaining)
        import sys as _sys

        _sys.stdout.write(
            f"\r{Fore.YELLOW}[{get_current_time()}] ⏱️  Очікування: залишилось {time_left}..."
        )
        _sys.stdout.flush()
        time.sleep(1)
    print()
