import time
from contextlib import asynccontextmanager
from typing import Optional

class Timer:
    def __init__(self, name: str):
        self.name = name
        self.start_ns: Optional[int] = None
        self.stages = []

    def start(self):
        self.start_ns = time.perf_counter_ns()
        return self

    def stage(self, stage_name: str):
        if self.start_ns is None:
            raise RuntimeError("Timer not started")
        current = time.perf_counter_ns()
        duration_ms = (current - self.start_ns) / 1_000_000
        self.stages.append((stage_name, duration_ms))
        return self

    def stop(self):
        if self.start_ns is None:
            raise RuntimeError("Timer not started")
        end_ns = time.perf_counter_ns()
        total_ms = (end_ns - self.start_ns) / 1_000_000
        
        print(f"\n=== {self.name} Performance Profile ===")
        for stage_name, duration in self.stages:
            print(f"{stage_name}: {duration:.3f} ms")
        print(f"Total time: {total_ms:.3f} ms\n")
        return total_ms
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False
    
    @property
    def elapsed(self):
        """Get elapsed time in milliseconds without stopping the timer"""
        if self.start_ns is None:
            return 0
        current_ns = time.perf_counter_ns()
        return (current_ns - self.start_ns) / 1_000_000

@asynccontextmanager
async def timer(name: str):
    t = Timer(name).start()
    try:
        yield t
    finally:
        t.stop()