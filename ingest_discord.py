import os
import asyncio
import discord
from discord.ext import commands
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, LongType
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GUILD_ID = int(os.getenv("GUILD_ID"))
FILE_PATH = os.getenv("FILE_PATH")  # Must be set in your .env file

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the bot with appropriate intents
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Define schema for the message data
schema = StructType([
    StructField("id", LongType(), False),
    StructField("content", StringType(), True),
    StructField("author", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("reactions", ArrayType(StructType([
        StructField("emoji", StringType(), True),
        StructField("count", IntegerType(), True)
    ])), True)
])

# Batch size for processing messages
batch_size = 5000  # Adjust as needed

# Initialize an asyncio.Queue with a maximum size to implement backpressure
queue_max_size = 10000  # Adjust based on available memory and performance testing
message_queue = asyncio.Queue(maxsize=queue_max_size)

# Function to initialize and return a SparkSession
def get_spark_session():
    return SparkSession.builder \
        .appName('DiscordMessageIngestion') \
        .master('local[*]') \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

# Function to save message data to a local Parquet file
def save_to_parquet(batch):
    spark = get_spark_session()
    try:
        df = spark.createDataFrame(batch, schema=schema)
        # Write DataFrame to Parquet without forcing repartitioning
        df.write.mode('append').parquet(FILE_PATH)
        logger.info(f"Saved batch of {len(batch)} messages to Parquet.")
    except Exception as e:
        logger.error(f"Error saving to Parquet: {str(e)}")
    finally:
        spark.stop()

# Asynchronous function to process messages from the queue
async def process_messages():
    batch = []
    while True:
        try:
            # Wait for a message from the queue
            message_data = await message_queue.get()

            if message_data is None:
                # Sentinel value indicating that fetching is complete
                break

            batch.append(message_data)

            # Process batch if batch size is reached
            if len(batch) >= batch_size:
                # Offload synchronous I/O to a thread to avoid blocking the event loop
                await asyncio.to_thread(save_to_parquet, batch.copy())
                batch.clear()

        except Exception as e:
            logger.error(f"Error processing messages: {str(e)}")

    # Process any remaining messages in the batch
    if batch:
        await asyncio.to_thread(save_to_parquet, batch.copy())
        batch.clear()

# Collect messages and reactions from a channel
async def fetch_channel_messages(channel):
    last_message = None  # Initialize as None for the first fetch
    total_fetched = 0
    retry_delay = 5  # Initial retry delay for general network errors

    while True:
        try:
            # Fetch messages, using `last_message` to paginate
            if last_message:
                messages = [message async for message in channel.history(limit=100, before=last_message)]
            else:
                messages = [message async for message in channel.history(limit=100)]
        except discord.errors.HTTPException as http_error:
            if http_error.status == 429:
                retry_after = int(http_error.response.headers.get('Retry-After', 5))
                logger.warning(f"Rate limited while fetching from {channel.name}. Waiting {retry_after} seconds.")
                await asyncio.sleep(retry_after)
                continue
            else:
                logger.error(f"HTTP error while fetching messages from {channel.name}: {str(http_error)}")
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)  # Exponential backoff up to 60 seconds
                continue
        except (discord.errors.ConnectionClosed, asyncio.TimeoutError) as network_error:
            logger.error(f"Network error while fetching messages from {channel.name}: {str(network_error)}")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)
            continue

        # Reset retry delay after a successful request
        retry_delay = 5

        if not messages:
            break

        for message in messages:
            # Collect message details
            message_data = {
                "id": message.id,
                "content": message.content,
                "author": str(message.author),
                "timestamp": str(message.created_at),
                "reactions": []
            }

            # Collect reactions without fetching users
            for reaction in message.reactions:
                message_data["reactions"].append({
                    "emoji": str(reaction.emoji),
                    "count": reaction.count
                })

            # Put message data into the queue, waiting if the queue is full
            await message_queue.put(message_data)

        # Set `last_message` for pagination
        last_message = messages[-1] if messages else None
        total_fetched += len(messages)
        logger.info(f"Fetched {total_fetched} messages from channel: {channel.name}")

# Fetch messages from all text channels in a guild
@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user.name}')
    guild = discord.utils.get(bot.guilds, id=GUILD_ID)
    if guild is None:
        logger.error(f"Guild with ID {GUILD_ID} not found.")
        await bot.close()
        return

    logger.info(f"Processing {len(guild.text_channels)} channels...")

    # Start the message processing task
    processor_task = asyncio.create_task(process_messages())

    # Limit the number of concurrent fetchers
    concurrency_limit = 5  # Adjust based on testing and system capabilities
    semaphore = asyncio.Semaphore(concurrency_limit)

    async def semaphore_wrapper(channel):
        async with semaphore:
            await fetch_channel_messages(channel)

    try:
        # Fetch messages from all channels concurrently, limited by the semaphore
        tasks = [semaphore_wrapper(channel) for channel in guild.text_channels]
        await asyncio.gather(*tasks)
    finally:
        # Signal the processor to stop after fetching is complete
        await message_queue.put(None)
        # Wait for the processor task to finish
        await processor_task
        logger.info("Finished fetching and processing messages.")
        await bot.close()

# Start the bot
bot.run(DISCORD_BOT_TOKEN)
