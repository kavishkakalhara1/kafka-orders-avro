import os
import shutil
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

OUTPUT_DIR = Path("demo_output")
SCREENSHOTS_DIR = OUTPUT_DIR / "screenshots"
VIDEOS_DIR = OUTPUT_DIR / "videos"

SCREENSHOTS_DIR.mkdir(parents=True, exist_ok=True)
VIDEOS_DIR.mkdir(parents=True, exist_ok=True)

def run():
    print("Starting Playwright demo recording...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1600, "height": 1050},
            record_video_dir=str(VIDEOS_DIR),
            record_video_size={"width": 1600, "height": 1050},
        )
        page = context.new_page()

        print("[1/7] Navigating to dashboard at http://127.0.0.1:8090...")
        page.goto("http://127.0.0.1:8090", wait_until="networkidle")
        page.wait_for_selector("#broker-status")
        time.sleep(2)
        page.screenshot(path=str(SCREENSHOTS_DIR / "01_initial_state.png"))
        print("  -> Saved 01_initial_state.png")

        print("[2/7] Starting Consumer and Retry workers...")
        page.click("#start-workers")
        page.wait_for_selector("#consumer-status.running", timeout=10000)
        page.wait_for_selector("#retry-status.running", timeout=10000)
        time.sleep(2.5)
        page.screenshot(path=str(SCREENSHOTS_DIR / "02_workers_started.png"))
        print("  -> Saved 02_workers_started.png")

        print("[3/7] Producing 5 normal orders (testing Avro serialization & running averages)...")
        page.select_option("#mode", "normal")
        page.fill("#count", "5")
        page.fill("#rate", "1")
        page.click("#produce-button")
        time.sleep(6.5)
        page.screenshot(path=str(SCREENSHOTS_DIR / "03_normal_orders_processed.png"))
        print("  -> Saved 03_normal_orders_processed.png")

        print("[4/7] Producing transient failure order (2 failures before recovery) + normal concurrent orders...")
        page.select_option("#mode", "transient")
        page.fill("#count", "1")
        page.fill("#failures", "2")
        page.click("#produce-button")
        time.sleep(1.2)

        # Switch to Retries tab
        page.click('button.tab[data-topic="retry"]')
        time.sleep(1.5)

        # Concurrently send 2 normal orders to prove non-blocking execution
        page.select_option("#mode", "normal")
        page.fill("#count", "2")
        page.click("#produce-button")
        time.sleep(5)
        page.screenshot(path=str(SCREENSHOTS_DIR / "04_transient_failure_and_recovery.png"))
        print("  -> Saved 04_transient_failure_and_recovery.png")

        print("[5/7] Injecting permanent errors (negative price, missing product, malformed payload)...")
        # Negative price
        page.select_option("#mode", "negative")
        page.fill("#count", "1")
        page.click("#produce-button")
        time.sleep(1.8)

        # Missing required product
        page.select_option("#mode", "missing-product")
        page.fill("#count", "1")
        page.click("#produce-button")
        time.sleep(1.8)

        # Malformed payload
        page.select_option("#mode", "malformed")
        page.fill("#count", "1")
        page.click("#produce-button")
        time.sleep(2)

        # Inspect Dead letters tab
        page.click('button.tab[data-topic="dlq"]')
        time.sleep(2)
        page.screenshot(path=str(SCREENSHOTS_DIR / "05_dead_letter_queue.png"))
        print("  -> Saved 05_dead_letter_queue.png")

        # Open inspect dialog on first DLQ record
        first_inspect = page.locator("button.inspect-button").first
        if first_inspect.is_visible():
            first_inspect.click()
            time.sleep(2.5)
            page.screenshot(path=str(SCREENSHOTS_DIR / "06_dlq_inspector_modal.png"))
            print("  -> Saved 06_dlq_inspector_modal.png")
            page.click("#close-dialog")
            time.sleep(1)

        print("[6/7] Testing retry budget exhaustion (3 attempts ending in DLQ)...")
        page.select_option("#mode", "exhausted")
        page.fill("#count", "1")
        page.click("#produce-button")
        page.click('button.tab[data-topic="retry"]')
        # Wait for retries to cycle through 1, 2, 3 with jitter (~8-10 seconds)
        time.sleep(9)
        page.click('button.tab[data-topic="dlq"]')
        time.sleep(2)
        page.screenshot(path=str(SCREENSHOTS_DIR / "07_retry_exhausted_to_dlq.png"))
        print("  -> Saved 07_retry_exhausted_to_dlq.png")

        print("[7/7] Exploring Schema Registry, Event Log, and Consumer Offsets...")
        # Schema view
        page.click('button.nav-item[data-view="schema"]')
        time.sleep(2.5)
        page.screenshot(path=str(SCREENSHOTS_DIR / "08_schema_registry.png"))
        print("  -> Saved 08_schema_registry.png")

        # Event log
        page.click('button.nav-item[data-view="events"]')
        time.sleep(2.5)
        page.screenshot(path=str(SCREENSHOTS_DIR / "09_event_logs.png"))
        print("  -> Saved 09_event_logs.png")

        # Overview and offsets
        page.click('button.nav-item[data-view="overview"]')
        page.click('button.tab[data-topic="all"]')
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)
        page.screenshot(path=str(SCREENSHOTS_DIR / "10_final_overview_and_offsets.png"))
        print("  -> Saved 10_final_overview_and_offsets.png")

        # Scroll back to top for final summary frame
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(2)

        # Close page and context to finalize video
        video_path = page.video.path()
        context.close()
        browser.close()

        final_video = OUTPUT_DIR / "demo_recording.webm"
        if Path(video_path).exists():
            shutil.move(video_path, str(final_video))
            print(f"Final video recording successfully saved to: {final_video}")
        else:
            print(f"Video file located at: {video_path}")

    print("Demo recording and captures completed successfully!")

if __name__ == "__main__":
    run()
