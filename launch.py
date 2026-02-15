"""
LoRaLlama Launcher — starts both the bridge and dashboard together.
Ctrl+C kills both processes cleanly.

Usage:
    python launch.py                          # defaults (BLE + Ollama)
    python launch.py --serial COM4            # serial connection
    python launch.py --llm anthropic          # different LLM provider
    python launch.py --no-dashboard           # bridge only

All unknown args are forwarded to llm_mesh_bridge.py.
"""

import subprocess
import sys
import signal
import os
import time

PYTHON = sys.executable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BRIDGE_SCRIPT = os.path.join(SCRIPT_DIR, "llm_mesh_bridge.py")
DASHBOARD_SCRIPT = os.path.join(SCRIPT_DIR, "dashboard.py")

processes = []


def cleanup(sig=None, frame=None):
    """Kill all child processes."""
    print("\n[LAUNCHER] Shutting down...")
    for name, proc in processes:
        if proc.poll() is None:
            print(f"[LAUNCHER] Stopping {name} (pid {proc.pid})")
            proc.terminate()
    # Give them a moment, then force kill
    deadline = time.time() + 5
    for name, proc in processes:
        remaining = max(0, deadline - time.time())
        try:
            proc.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            print(f"[LAUNCHER] Force killing {name}")
            proc.kill()
    print("[LAUNCHER] All processes stopped.")
    sys.exit(0)


def main():
    # Parse our own flag, pass everything else to the bridge
    no_dashboard = "--no-dashboard" in sys.argv
    bridge_args = [a for a in sys.argv[1:] if a != "--no-dashboard"]

    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    # Start dashboard first (unless disabled)
    # Dashboard output goes to a log file so it doesn't stomp on bridge prompts
    if not no_dashboard:
        dash_log_path = os.path.join(SCRIPT_DIR, "dashboard.log")
        dash_log = open(dash_log_path, "w", encoding="utf-8", errors="replace")
        print(f"[LAUNCHER] Starting dashboard on http://localhost:5000  (logs: dashboard.log)")
        dash_env = os.environ.copy()
        dash_env["PYTHONIOENCODING"] = "utf-8"
        dash_proc = subprocess.Popen(
            [PYTHON, DASHBOARD_SCRIPT],
            cwd=SCRIPT_DIR,
            stdout=dash_log,
            stderr=dash_log,
            env=dash_env,
        )
        processes.append(("dashboard", dash_proc))
        time.sleep(1)

    # Start bridge (with any extra args forwarded)
    print(f"[LAUNCHER] Starting bridge: {' '.join(bridge_args) or '(defaults)'}")
    bridge_proc = subprocess.Popen(
        [PYTHON, BRIDGE_SCRIPT] + bridge_args,
        cwd=SCRIPT_DIR,
    )
    processes.append(("bridge", bridge_proc))

    # Wait for either to exit — then tear down both
    while True:
        for name, proc in processes:
            ret = proc.poll()
            if ret is not None:
                print(f"\n[LAUNCHER] {name} exited (code {ret})")
                cleanup()
        time.sleep(0.5)


if __name__ == "__main__":
    main()
