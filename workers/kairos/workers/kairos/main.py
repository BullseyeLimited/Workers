import os, time

if not os.getenv("RUNPOD_URL"):
    print("No model endpoint yet – sleeping.")
    while True:
        time.sleep(60)
