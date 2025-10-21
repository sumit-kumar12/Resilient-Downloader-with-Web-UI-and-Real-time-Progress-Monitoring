# TODO: Integrate HTML Frontend with FastAPI Backend

- [x] Add WebSocket connection to ws://127.0.0.1:8000/ws in the script
- [x] Handle incoming WebSocket messages to update UI dynamically (downloads, stats, logs)
- [x] Modify addDownload function to send POST request to /api/add with JSON payload
- [x] Remove demo data initialization and rely on WebSocket for real-time updates
- [x] Update render functions to match backend data structure (downloads: id, filename, status, progress, speed, eta, total_size, etc.)
- [x] Update stats rendering to use backend system data (speed, active downloads, disk free)
- [x] Keep logs as local for user actions
- [x] Test the integration: run start_app.py, add download, verify UI updates
