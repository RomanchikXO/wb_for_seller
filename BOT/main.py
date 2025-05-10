from BOT.loader_bot import bot
import handlers  # noqa
from utils.set_bot_commands import set_default_commands
import logging
from context_logger import ContextLogger

logger = ContextLogger(logging.getLogger("bot"))


if __name__ == "__main__":
    set_default_commands(bot)
    logger.info("Бот запущен")
    bot.infinity_polling(skip_pending=True)