"""
Scheduler for periodic data import from the 1C API.

This file contains the scheduler that runs the import process at regular intervals
to ensure that the data in the database is always up-to-date.
"""

import asyncio
import logging
from datetime import datetime
import time
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
import sds_import
from sqlalchemy.future import select
from sqlalchemy import delete
from models import Product, ProductCharacteristic, ProductAnalog, ProductBarcode, ProductCertificate, ProductInstruction, ProductPhoto, ProductPrice
from db import AsyncSessionLocal

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("import_log.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sds_import_scheduler")

async def cleanup_removed_products(processed_articles):
    """
    Remove products that are no longer available in the API.
    
    Args:
        processed_articles: A set of article IDs that were processed during the import
    """
    logger.info("Starting cleanup of removed products")
    start_time = time.time()
    
    async with AsyncSessionLocal() as session:
        # Get all articles from the database
        stmt = select(Product.article)
        result = await session.execute(stmt)
        db_articles = {row[0] for row in result.all()}
        
        # Find articles that are in the database but not in the processed set
        articles_to_remove = db_articles - processed_articles
        
        if articles_to_remove:
            logger.info(f"Found {len(articles_to_remove)} products to remove")
            
            # Delete products that are no longer in the API
            for article in articles_to_remove:
                # Find the product
                stmt = select(Product).where(Product.article == article)
                result = await session.execute(stmt)
                product = result.scalar_one_or_none()
                
                if product:
                    # Log the removal
                    logger.info(f"Removing product {article} (ID: {product.id})")
                    
                    # Delete the product (cascade will handle related records)
                    await session.delete(product)
            
            # Commit the changes
            await session.commit()
            logger.info(f"Removed {len(articles_to_remove)} products that are no longer in the API")
        else:
            logger.info("No products to remove")
    
    elapsed = time.time() - start_time
    logger.info(f"Cleanup completed in {elapsed:.2f} seconds")

async def run_import():
    """
    Run the import process and track which products were processed.
    """
    logger.info("Starting scheduled import process")
    start_time = time.time()
    
    try:
        # Run the import process and get the set of processed articles
        processed_articles = await sds_import.main(return_processed_articles=True)
        
        # Clean up products that are no longer in the API
        await cleanup_removed_products(processed_articles)
        
        elapsed = time.time() - start_time
        logger.info(f"Import process completed successfully in {elapsed:.2f} seconds")
    except Exception as e:
        logger.error(f"Error during import process: {str(e)}", exc_info=True)

def start_scheduler():
    """
    Start the scheduler to run the import process at regular intervals.
    """
    scheduler = AsyncIOScheduler()
    
    # Schedule the import process to run every day at 2 AM
    scheduler.add_job(
        run_import,
        CronTrigger(hour=2, minute=0),
        id="daily_import",
        replace_existing=True
    )
    
    # Start the scheduler
    scheduler.start()
    logger.info("Scheduler started. Import process will run daily at 2 AM.")
    
    return scheduler

if __name__ == "__main__":
    # Set up the event loop
    loop = asyncio.get_event_loop()
    
    # Start the scheduler
    scheduler = start_scheduler()
    
    try:
        # Run the import process immediately on startup
        loop.run_until_complete(run_import())
        
        # Keep the script running
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
    finally:
        scheduler.shutdown()
        loop.close()