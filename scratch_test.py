import asyncio
import aiohttp

async def test():
    url = "https://cdn.discordapp.com/attachments/1496190438850171051/1496190600000000000/test.png"  # dummy URL
    # Let's test with a real public image URL to see if connection/SSL works
    url = "https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png"
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    try:
        connector = aiohttp.TCPConnector(limit=10, ssl=False)
        print("Connector created successfully.")
    except Exception as e:
        print("Failed to create connector:", e)
        return
        
    try:
        async with aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=60)
        ) as download_session:
            print("Session created successfully.")
            async with download_session.get(url) as resp:
                print("Response status:", resp.status)
                content = await resp.read()
                print("Content length:", len(content))
    except Exception as e:
        print("Request failed:", e)

asyncio.run(test())
