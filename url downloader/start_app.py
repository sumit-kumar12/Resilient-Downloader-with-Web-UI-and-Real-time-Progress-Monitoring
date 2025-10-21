#start_app.py
#!/usr/bin/env python3
"""
Auto-launch script for Super Downloader
"""
import os, sys, subprocess, time, webbrowser

CREATE_NEW_CONSOLE = 0x00000010

def main():
    print("Installing dependencies from requirements.txt...")
    subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], check=True)

    print("Starting backend server in a new console window...")
    # Use CREATE_NEW_CONSOLE to run the backend in a separate window.
    # This allows you to see the server's logs while it runs independently.
    # The server will keep running even if this script's window is closed.
    subprocess.Popen([sys.executable, 'main.py'], creationflags=CREATE_NEW_CONSOLE)

    print("Waiting for server to initialize...")
    time.sleep(2)
    print("Opening dashboard in web browser...")
    webbrowser.open(f"file://{os.path.abspath('download8.html')}")

if __name__ == "__main__":
    main()
