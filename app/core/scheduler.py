from apscheduler.schedulers.background import BackgroundScheduler
from app.db.session import SessionLocal
from app.models.stock import Stock
from app.services.stock_service import StockService
from datetime import datetime, timedelta, timedelta

from app.core.redis import get_redis_client

scheduler = BackgroundScheduler()

def sync_all_stocks_job():
    print(f"Starting stock list sync job at {datetime.now()}")
    
    # Check if already ran today
    redis_client = get_redis_client()
    today = datetime.now().strftime("%Y-%m-%d")
    key = "stock:sync:last_run_date"
    
    try:
        last_run = redis_client.get(key)
        if last_run == today:
            print(f"Stock list already synced today ({today}). Skipping.")
            return
    except Exception as e:
        print(f"Redis error checking sync status: {e}")
        # If redis fails, maybe we still run? Or skip to be safe? 
        # Let's run to ensuring data is fresh if redis is down, or maybe fail?
        # Safe to run if redis is down, just might duplicate.
        pass

    db = SessionLocal()
    try:
        service = StockService(db)
        count = service.sync_all_stocks()
        print(f"Synced {count} stocks")
        # Update last run date
        try:
            redis_client.set(key, today, ex=86400 * 2) # Expire in 2 days
        except Exception as e:
            print(f"Failed to set redis sync key: {e}")
            
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
