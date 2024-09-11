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
FILE_PATH = os.getenv("FILE_PATH")  # No default, must be set in .env

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the bot with appropriate intents
intents = discord.Intents.default()
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Initialize Spark session with Hadoop dependencies disabled
spark = SparkSession.builder \
    .appName('DiscordMessageIngestion') \
    .config("spark.hadoop.fs.defaultFS", "file:///") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

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

# Initialize an empty list to store the batch of messages
batch = []
batch_size = 1000

# Semaphore for controlling concurrency
concurrency_limit = 2  # Adjust based on testing
semaphore = asyncio.Semaphore(concurrency_limit)

# Function to save message data to a local Parquet file
def save_to_parquet(df, file_path):
    # Write the DataFrame to Parquet without forcing repartitioning
    df.write.mode('append').parquet(file_path)
    logger.info(f"Saved batch of {df.count()} messages to Parquet.")

# Collect messages and reactions from a channel
async def fetch_channel_messages(channel):
    global batch
    last_message = None  # Initialize as None for the first fetch
    total_fetched = 0
    retry_delay = 5  # Initial retry delay set to 5 seconds

    try:
        while True:
            async with semaphore:
                try:
                    # Fetch messages, using `last_message` to paginate
                    if last_message:
                        messages = [message async for message in channel.history(limit=100, before=last_message)]
                    else:
                        messages = [message async for message in channel.history(limit=100)]
                
                except (discord.errors.HTTPException, discord.errors.ConnectionClosed, asyncio.TimeoutError) as network_error:
                    logger.error(f"Network error while fetching messages from {channel.name}: {str(network_error)}")
                    await asyncio.sleep(retry_delay)  # Wait before retrying
                    retry_delay = min(retry_delay * 2, 60)  # Exponentially increase delay up to max of 60 seconds
                    continue  # Retry the loop after encountering a network error

                retry_delay = 5  # Reset retry delay after a successful request

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

                    # Add message data to the batch
                    batch.append(message_data)

                    # Write to Parquet if batch size is reached
                    if len(batch) >= batch_size:
                        batch_df = spark.createDataFrame(batch, schema=schema)
                        save_to_parquet(batch_df, FILE_PATH)
                        batch = []  # Clear the batch after saving

                # Set `last_message` for pagination
                last_message = messages[-1] if messages else None
                total_fetched += len(messages)
                logger.info(f"Fetched {total_fetched} messages from channel: {channel.name}")

        # Write remaining messages in the batch to Parquet
        if batch:
            batch_df = spark.createDataFrame(batch, schema=schema)
            save_to_parquet(batch_df, FILE_PATH)
            logger.info(f"Saved remaining batch of {len(batch)} messages to Parquet.")

    except Exception as e:
        # Handle other errors and log them
        logger.error(f"Error fetching messages from channel {channel.name}: {str(e)}")

# Fetch messages from all text channels in a guild
@bot.event
async def on_ready():
    logger.info(f'Logged in as {bot.user.name}')
    guild = discord.utils.get(bot.guilds, id=GUILD_ID)
    logger.info(f"Processing {len(guild.text_channels)} channels...")

    try:
        # Fetch messages from all channels concurrently
        tasks = [fetch_channel_messages(channel) for channel in guild.text_channels]
        await asyncio.gather(*tasks)
        logger.info("Finished fetching messages.")
    finally:
        # Ensure Spark session is stopped after processing is complete
        spark.stop()
        logger.info("Spark session stopped.")
        await bot.close()

# Start the bot with the token from the .env file
bot.run(DISCORD_BOT_TOKEN)
