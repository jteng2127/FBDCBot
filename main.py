from discord_bot import bot
import os
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    os.chdir(os.path.dirname(__file__))
    bot.run(os.getenv('DISCORD_BOT_TOKEN'))