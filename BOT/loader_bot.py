from telebot import TeleBot
from loader import BOT_TOKEN


bot = TeleBot(token=BOT_TOKEN, skip_pending=True)


