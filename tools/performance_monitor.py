#!/usr/bin/env python3
"""
Real-time performance monitor for prepare_dataset.py
Tracks CPU, memory, and progress statistics during processing.
"""

import psutil
import time
import threading
import os
import sys
from datetime import datetime, timedelta

class PerformanceMonitor:
    def __init__(self, target_process_name="prepare_dataset.py"):
        self.target_process_name = target_process_name
        self.monitoring = False
        self.start_time = None
        self.peak_memory = 0
        self.peak_cpu = 0

    def find_target_process(self):
        """Find the prepare_dataset.py process."""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if any(self.target_process_name in arg for arg in proc.info['cmdline']):
                    return psutil.Process(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return None

    def format_bytes(self, bytes_val):
        """Format bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.1f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.1f} TB"

    def monitor_performance(self):
        """Monitor system and process performance."""
        print("🔍 Looking for prepare_dataset.py process...")

        while self.monitoring:
            target_proc = self.find_target_process()

            if target_proc:
                if not self.start_time:
                    self.start_time = datetime.now()
                    print(f"✅ Found process {target_proc.pid}")
                    print("📊 Starting performance monitoring...")
                    print("=" * 60)

                try:
                    # Process statistics
                    proc_memory = target_proc.memory_info()
                    proc_cpu = target_proc.cpu_percent()

                    # System statistics
                    system_memory = psutil.virtual_memory()
                    system_cpu = psutil.cpu_percent(interval=1)

                    # Update peaks
                    memory_gb = proc_memory.rss / (1024**3)
                    self.peak_memory = max(self.peak_memory, memory_gb)
                    self.peak_cpu = max(self.peak_cpu, proc_cpu)

                    # Runtime calculation
                    runtime = datetime.now() - self.start_time

                    # Clear screen and display stats
                    os.system('clear' if os.name == 'posix' else 'cls')

                    print("🚀 PREPARE_DATASET.PY PERFORMANCE MONITOR")
                    print("=" * 60)
                    print(f"⏱️  Runtime: {str(runtime).split('.')[0]}")
                    print(f"🆔 Process ID: {target_proc.pid}")
                    print()

                    print("📊 PROCESS STATISTICS:")
                    print(f"   💾 Memory Usage: {self.format_bytes(proc_memory.rss)} (Peak: {self.peak_memory:.1f} GB)")
                    print(f"   🔥 CPU Usage: {proc_cpu:.1f}% (Peak: {self.peak_cpu:.1f}%)")
                    print(f"   📈 VMS: {self.format_bytes(proc_memory.vms)}")
                    print()

                    print("🖥️  SYSTEM STATISTICS:")
                    print(f"   💾 Total RAM: {self.format_bytes(system_memory.total)}")
                    print(f"   💾 Used RAM: {self.format_bytes(system_memory.used)} ({system_memory.percent:.1f}%)")
                    print(f"   💾 Available: {self.format_bytes(system_memory.available)}")
                    print(f"   🔥 System CPU: {system_cpu:.1f}%")
                    print(f"   🔥 CPU Cores: {psutil.cpu_count()} physical, {psutil.cpu_count(logical=True)} logical")
                    print()

                    # Memory efficiency calculation
                    memory_efficiency = (memory_gb / (system_memory.total / (1024**3))) * 100
                    print("⚡ EFFICIENCY METRICS:")
                    print(f"   📊 Memory Efficiency: {memory_efficiency:.1f}% of total RAM")
                    print(f"   🚀 Processing Rate: {self.estimate_processing_rate(runtime)}")

                    # Performance warnings
                    if memory_gb > 80:  # > 80GB
                        print("   ⚠️  HIGH MEMORY USAGE - Consider reducing batch size")
                    if system_memory.percent > 95:
                        print("   🔴 CRITICAL MEMORY - System may become unstable")
                    if proc_cpu < 50:
                        print("   💡 LOW CPU USAGE - Consider increasing workers")

                    print()
                    print("=" * 60)
                    print("Press Ctrl+C to stop monitoring")

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    print("❌ Process ended or access denied")
                    break

            else:
                if self.start_time:
                    print("\n✅ Process completed!")
                    runtime = datetime.now() - self.start_time
                    print(f"📊 Total runtime: {str(runtime).split('.')[0]}")
                    print(f"💾 Peak memory: {self.peak_memory:.1f} GB")
                    print(f"🔥 Peak CPU: {self.peak_cpu:.1f}%")
                    break
                else:
                    print("🔍 Waiting for prepare_dataset.py to start...")
                    time.sleep(2)
                    continue

            time.sleep(1)  # Update every second

    def estimate_processing_rate(self, runtime):
        """Estimate processing rate based on runtime."""
        minutes = runtime.total_seconds() / 60
        if minutes < 1:
            return "Initializing..."
        elif minutes < 5:
            return "Loading data..."
        else:
            # Rough estimate based on typical workloads
            estimated_chains_per_minute = 1000  # Adjust based on your system
            return f"~{estimated_chains_per_minute * minutes:.0f} chains processed"

    def start_monitoring(self):
        """Start the monitoring process."""
        self.monitoring = True
        monitor_thread = threading.Thread(target=self.monitor_performance)
        monitor_thread.daemon = True
        monitor_thread.start()

        try:
            while self.monitoring:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped by user")
            self.monitoring = False

def main():
    print("🚀 Performance Monitor for prepare_dataset.py")
    print("💡 This tool monitors CPU, memory, and performance metrics")
    print("📊 Start prepare_dataset.py in another terminal to begin monitoring")
    print()

    monitor = PerformanceMonitor()
    monitor.start_monitoring()

if __name__ == "__main__":
    main()
