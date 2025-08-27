from loader_bot import bot
from telebot.types import Message
from states import get_status, set_status


@bot.message_handler(commands=["start"])
def start_handler(message: Message):
    bot.send_message(message.chat.id, f"Привет, я Бот! Твой ТГ ID: {message.from_user.id}")


@bot.message_handler(func=lambda message: get_status(message.from_user.id) == 'get_sms_code')
def handle_sms_code(message: Message):
    user_id = message.from_user.id
    sms_code = message.text.strip()
    set_status(f"code_{sms_code}", user_id)