# Screen Time ETL: Deployment & Operations

This document serves as the technical record for the **Mac Screen Time Scraper** and the **AWS Lambda Ingestion Endpoint** for the `doug-dashboard`.

## 1. System Architecture
The pipeline utilizes a hybrid **Pull/Push** model to overcome the inherent "lazy-sync" limitations of Apple's `knowledgeC.db`.



* **Mac Source:** A local Python script extracts the last 30 days of usage records from the forensic `knowledgeC.db`.
* **Persistence:** Managed via macOS `launchd` to ensure execution even after the computer wakes from sleep.
* **Ingestion:** A secure AWS Lambda endpoint validates requests via a Bearer Token and stores partitioned JSONs in S3.

---

## 2. Local Mac Deployment (`launchd`)

### Current File Locations
* **Active Directory:** `/Users/dougkim/screen_time_job/`
* **Script Name:** `local-screentime-collection-script.py`
* **Launch Agent:** `~/Library/LaunchAgents/com.doug.screentime.plist`

### Example XML Definition (`.plist`)
This file must be placed in `~/Library/LaunchAgents/` to run as your user. It uses absolute paths to ensure the background manager finds the correct environment.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "[http://www.apple.com/DTDs/PropertyList-1.0.dtd](http://www.apple.com/DTDs/PropertyList-1.0.dtd)">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.doug.screentime</string>
    <key>ProgramArguments</key>
    <array>
        <string>/Users/dougkim/dev/doug-dashboard/.venv/bin/python</string>
        <string>/Users/dougkim/screen_time_job/local-screentime-collection-script.py</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>AUTH_TOKEN</key>
        <string>your_bearer_token_here</string>
        <key>SCREENTIME_LAMBDA_ENDPOINT</key>
        <string>https://your-lambda-url.amazonaws.com/</string>
    </dict>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>1</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/tmp/com.doug.screentime.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/com.doug.screentime.stderr.log</string>
    <key>RunAtLoad</key>
    <true/>
</dict>
</plist>
```

## 3. Operations & Maintenance

### How to Re-Deploy
Whenever the script or the `.plist` configuration is updated, follow these steps to refresh the background agent:

1.  **Modify the Plist:** Update paths or environment variables in `~/Library/LaunchAgents/com.doug.screentime.plist`.
2.  **Reset the Service:**
    ```bash
    launchctl unload ~/Library/LaunchAgents/com.doug.screentime.plist
    launchctl load ~/Library/LaunchAgents/com.doug.screentime.plist
    ```
3.  **Manual Trigger (Test):**
    ```bash
    # Forces an immediate run to verify configuration
    launchctl kickstart -p gui/$(id -u)/com.doug.screentime
    ```

---

### Audit Trails & Logging
To ensure visibility into background executions, the system uses two layers of logging:

* **Application Logs**: Execution logs are partitioned by date in `../logs/YYYY-MM-DD/screen_time_scraper.log`. This includes the full response body from the Lambda endpoint for every successful or failed push.
* **System Logs**: Initialization and environment errors are captured by `launchd` in `/tmp/com.doug.screentime.stderr.log`.

---

### State Management & Backfill
The system is designed to be resilient against intermittent downtime or sleep cycles.

* **State Tracking**: A local JSON file at `~/.screen_time_state.json` maintains a manifest of all dates successfully processed and pushed to S3.
* **Self-Healing Logic**: On every execution, the script performs a set-difference between the last 30 days (the SQLite retention limit) and the processed dates in the state file.
* **Automatic Recovery**: If the Mac is powered off or disconnected for several days, the next successful run will automatically iterate through all missing dates to backfill the data lake.

---

### Manual Maintenance Tasks
* **Full Re-sync**: To force a complete re-examination and overwrite of the 30-day window, delete the `~/.screen_time_state.json` file and trigger a manual `kickstart`.
* **Log Rotation**: It is recommended to periodically clear the `logs/` directory to manage local disk space, as partitioned logs will accumulate daily.