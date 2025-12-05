from apscheduler.schedulers.background import BackgroundScheduler
from app.db.session import SessionLocal
from app.models.stock import Stock
from app.services.stock_service import StockService
from datetime import datetime, timedelta, timedelta

scheduler = BackgroundScheduler()

def sync_all_stocks_job():
    print(f"Starting stock list sync job at {datetime.now()}")
    db = SessionLocal()
    try:
        service = StockService(db)
        count = service.sync_all_stocks()
        print(f"Synced {count} stocks")
    except Exception as e:
        print(f"Failed to sync stocks: {e}")
    finally:
        db.close()
    print(f"Finished stock list sync job at {datetime.now()}")

def start_scheduler():
    # Run every day at 1 AM
    scheduler.add_job(sync_all_stocks_job, 'cron', hour=1, minute=0)
    # Also run once on startup for demo purposes (optional, but good for verification)
    scheduler.add_job(sync_all_stocks_job, 'date', run_date=datetime.now() + timedelta(seconds=5))
    scheduler.start()
