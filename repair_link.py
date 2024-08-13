import asyncio
import pyppeteer
from pyppeteer.errors import TimeoutError
import logging
from lxml import etree

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_product_sitemap(domain):
    #create search browser
    try:
        browser = await pyppeteer.launch(
            headless=False,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--ignore-certificate-errors',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            ]
        )
    except Exception as e:
        logger.error(f"Failed to launch browser: {e}")
        return
    #list for all products links
    products_links = []
    try:
        #create web page
        page = await browser.newPage()
        await page.setViewport({"width": 1280, "height": 800})
        await page.goto(f"https://{domain}/sitemap.xml", {"waitUntil": "networkidle2"})
        # Extract all <loc> elements and their text content
        links = await page.evaluate('''() => {
                // Get all <loc> elements
                const locElements = document.querySelectorAll('loc');
                // Extract and return the text content of each <loc> element
                return Array.from(locElements).map(element => element.textContent.trim());
            }''')
        # Print the extracted links
        for link in links:
            if "sitemap_products" in link or "sitemap_collections" in link:
                await page.goto(link, {"waitUntil": "networkidle2"})
                products = await page.evaluate('''() => {
                    // Select all <loc> elements that are children of <url> elements
                    const locElements = document.querySelectorAll('url > loc');
                    // Extract and return the text content of each <loc> element
                    return Array.from(locElements).map(loc => loc.textContent.trim());
                }''')
                products_links += products

    except TimeoutError:
        logger.error("Timeout while trying to load the sitemap.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        await browser.close()
        return products_links
#
# if __name__ == "__main__":
#     asyncio.get_event_loop().run_until_complete(get_product_sitemap("1212getgive.com"))