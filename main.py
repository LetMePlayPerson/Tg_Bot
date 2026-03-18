import datetime
import aiohttp
import json
import os
import re
import telegram
import logging
import sys
from dotenv import load_dotenv
from telegram import (Update,
                      ReplyKeyboardMarkup,
                      ReplyKeyboardRemove)
from telegram.ext import (Application,
                          MessageHandler,
                          CommandHandler,
                          filters,
                          CallbackContext)


# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),  # Логирование в файл
        logging.StreamHandler()  # Логирование в консоль
    ]
)

logger = logging.getLogger(__name__)


# Загружаем переменные из .env
load_dotenv()


# Настройки Telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
ADMIN_GROUP_ID = int(os.getenv('ADMIN_GROUP_ID'))
MATTERMOST_WEBHOOK_ACTIV = os.getenv('MATTERMOST_WEBHOOK_ACTIV')
MATTERMOST_WEBHOOK_SELL = os.getenv('MATTERMOST_WEBHOOK_SELL')
MATTERMOST_WEBHOOK_SPEND = os.getenv('MATTERMOST_WEBHOOK_SPEND')


if not TELEGRAM_TOKEN:
    logger.critical("Токен Telegram не найден в переменных окружения!")
    raise ValueError("Токен Telegram не найден в переменных окружения!")

logger.info("Конфигурация загружена успешно")


# Глобальные константы и переменные
DATA_FILE = 'user_topics.json'
MESSAGE_DELAY_SECONDS = 130

# Словарь для хранения времени последнего сообщения от пользователя
last_message_time = {}


# Загрузка данных из файла
def load_user_topics():
    """
    Загружает данные о топиках из файла.
    """
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as file:
                data = json.load(file)
                if not isinstance(data, dict):
                    logger.warning("user_topics.json имеет неверную структуру")
                    return {}
                logger.info(f"Загружено {len(data)} записей о топиках")
                return data
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}")
            # Переименуем битый файл, чтобы не потерять его
            timestamp = int(datetime.datetime.now().timestamp())
            backup_name = f"{DATA_FILE}.corrupted.{timestamp}"
            os.rename(DATA_FILE, backup_name)
            logger.info(f"Повреждённый файл переименован в {backup_name}")
            return {}
        except PermissionError:
            logger.error(f"Нет прав на чтение {DATA_FILE}")
            return {}
        except Exception as e:
            error_msg = f"Непредвиденная ошибка при загрузке: {e}"
            logger.error(error_msg, exc_info=True)
            return {}
    return {}


# Сохранение данных в файл
def save_user_topics(user_topics):
    """
    Сохраняет данные о топиках в файл.
    """
    try:
        # Сначала пишем во временный файл
        temp_file = f"{DATA_FILE}.tmp"
        with open(temp_file, 'w', encoding='utf-8') as file:
            json.dump(user_topics, file, ensure_ascii=False, indent=4)
        # Атомарно заменяем основной файл
        os.replace(temp_file, DATA_FILE)
        logger.debug(f"Сохранено {len(user_topics)} записей в {DATA_FILE}")
    except PermissionError:
        logger.error(f"Нет прав на запись в {DATA_FILE}")
    except Exception as e:
        logger.error(f"Ошибка сохранения {DATA_FILE}: {e}", exc_info=True)
        if os.path.exists(f"{DATA_FILE}.tmp"):
            os.remove(f"{DATA_FILE}.tmp")


# Словарь для хранения контекста диалогов
user_topics = load_user_topics()


# Словарь для хранения промежуточных данных пользователей
user_data = {}  # user_id -> {"step": str, "data": dict}

# Шаблоны клавиатур
KEYBOARD_SALE_ACTIVATION = [
    ['продажа'],
    ['активация'],
    ['активация + продажа'],
    ['спенд'],
    ['проверить статус запроса'],
]

KEYBOARD_MBB_KSN = [
    ['МББ'],
    ['КСН'],
]

KEYBOARD_MBB_KSN_NOTHING = [
    ['МББ'],
    ['КСН'],
    ['Без продажи'],
]


async def send_to_mm(mattermost_webhook_url: str, message: str):
    """
    Отправляет сообщение в Mattermost асинхронно.
    """
    logger.debug(f"Отправка сообщения в Mattermost: {message[:50]}...")

    payload = {
        'text': message
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                mattermost_webhook_url,
                json=payload,
            ) as response:
                if response.status != 200:
                    text = await response.text()
                    error_msg = f"ММ Ошибка: {response.status} - {text}"
                    logger.error(error_msg)
    except aiohttp.ClientError as e:
        logger.error(f"Сетевая ошибка при отправке в Mattermost: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке в Mattermost: {e}")


async def start(update: Update, context: CallbackContext):
    """
    Команда /start для начала работы с ботом.
    """
    user_id = update.message.from_user.id
    logger.info(f"Пользователь {user_id} запустил бота")

    current_time = datetime.datetime.now()

    if user_id in last_message_time:
        time_delta = current_time - last_message_time[user_id]
        time_since_last = time_delta.total_seconds()
        if time_since_last < MESSAGE_DELAY_SECONDS:
            wait_time = int(MESSAGE_DELAY_SECONDS - time_since_last) + 1
            await update.message.reply_text(
                f"Пожалуйста, подождите {wait_time} секунд(ы) "
                f"перед отправкой следующего сообщения."
            )
            return

    user_data[user_id] = {"step": "enter_code", "data": {}}
    await update.message.reply_text(
        "Введите лид вида 1-XXXXXXX:"
    )


async def handle_user_message(update: Update, context: CallbackContext):
    """
    Обрабатывает сообщения от пользователей.
    """
    message = update.message
    user_id = message.from_user.id
    text = message.text

    if user_id not in user_data:
        await message.reply_text(
            """Чтобы начать, используйте команду /start.
            \nЗапросы обрабатываются по будням с 6:00 до 19:00 по МСК"""
            )
        return

    step = user_data[user_id]["step"]

    if step == "enter_code":
        # Проверяем, соответствует ли код шаблону "1-XXXXXXX"
        if re.match(r"^1-[A-Za-z0-9]{7}$", text):
            user_data[user_id]["data"]["code"] = text
            user_data[user_id]["step"] = "choose_action"
            await message.reply_text(
                "Выберите действие:",
                reply_markup=ReplyKeyboardMarkup(KEYBOARD_SALE_ACTIVATION,
                                                 one_time_keyboard=True)
            )
        else:
            await message.reply_text(
                "Неверный лид. Введите лид в формате 1-XXXXXXX."
            )

    elif step == "choose_action":
        # Проверяем, что выбрано "продажа" или "активация"
        if text in [
            "продажа",
            "активация",
            "активация + продажа",
            "спенд",
            "проверить статус запроса"
        ]:
            user_data[user_id]["data"]["action"] = text
            if text == "продажа":
                user_data[user_id]["step"] = "choose_product"
                await message.reply_text(
                    "Выберите продукт:",
                    reply_markup=ReplyKeyboardMarkup(KEYBOARD_MBB_KSN,
                                                     one_time_keyboard=True)
                )
            elif text == "активация":
                user_data[user_id]["step"] = "choose_product"
                await message.reply_text(
                    "Выберите продукт:",
                    reply_markup=ReplyKeyboardMarkup(KEYBOARD_MBB_KSN_NOTHING,
                                                     one_time_keyboard=True)
                )
            elif text == "активация + продажа":
                user_data[user_id]["step"] = "choose_product"
                await message.reply_text(
                    "Выберите продукт:",
                    reply_markup=ReplyKeyboardMarkup(KEYBOARD_MBB_KSN,
                                                     one_time_keyboard=True)
                )
            elif text == "спенд":
                user_data[user_id]["step"] = "final"
                await finalize_message(user_id, message, context)

            elif text == "проверить статус запроса":
                user_data[user_id]["step"] = "final"
                await finalize_message(user_id, message, context)

            else:
                user_data[user_id]["step"] = "final"
                await finalize_message(user_id, message, context)

    elif step == "choose_product":
        # Проверяем, что выбрано "МББ" или "КСН"
        if text in ["МББ", "КСН"]:
            user_data[user_id]["data"]["product"] = text
            user_data[user_id]["step"] = "final"
            await finalize_message(user_id, message, context)
        elif text in ["Без продажи"]:
            user_data[user_id]["step"] = "final"
            await finalize_message(user_id, message, context)
        else:
            await message.reply_text(
                "Выберите продукт из предложенных вариантов."
            )


async def finalize_message(user_id: int, message: telegram.Message,
                           context: CallbackContext):
    """
    Формирует итоговое сообщение и отправляет его в топик.
    """
    logger.info(f"Обработка заявки от пользователя {user_id}")

    current_time = datetime.datetime.now()
    data = user_data[user_id]["data"]
    username = message.from_user.username or message.from_user.first_name

    # Формируем сообщение по шаблону
    final_message = f"{data['code']}\n{data['action']}"
    if "product" in data:
        final_message += f" {data['product']}"

    # Отправляем сообщение в топик
    try:
        # Отправляем сообщение в топик
        topic_id = await find_or_create_topic(context.bot, username, user_id)
        await context.bot.send_message(
            chat_id=ADMIN_GROUP_ID,
            text=final_message,
            message_thread_id=topic_id
        )
        logger.info(f"Сообщение отправлено в топик {topic_id}")

    except telegram.error.BadRequest as e:
        # Топик удалён, архивирован или бот не имеет прав
        logger.warning(
            f"Не удалось отправить в топик {topic_id}: {e}. Создаю новый..."
        )

        # Удаляем старую запись и создаём новый топик
        if str(user_id) in user_topics:
            del user_topics[str(user_id)]
            save_user_topics(user_topics)

        # Пробуем ещё раз с новым топиком
        try:
            topic_id = await create_topic(context.bot, username)
            user_topics[str(user_id)] = topic_id
            save_user_topics(user_topics)

            await context.bot.send_message(
                chat_id=ADMIN_GROUP_ID,
                text=final_message,
                message_thread_id=topic_id
            )
            logger.info(f"Сообщение отправлено в новый топик {topic_id}")
        except Exception as retry_error:
            logger.error(f"Повторная отправка не удалась: {retry_error}")
            await message.reply_text(
                "⚠️ Произошла ошибка при отправке заявки. Попробуйте позже."
            )
            return  # Прерываем выполнение, не отправляем в MM

    except telegram.error.Forbidden as e:
        # Бот удалён из группы или заблокирован
        logger.critical(f"Бот не имеет доступа к группе: {e}")
        await message.reply_text(
            "❌ Ошибка доступа. Обратитесь к администратору."
        )
        return

    except Exception as e:
        # Любая другая неожиданная ошибка
        logger.error(f"Непредвиденная ошибка при отправке: {e}", exc_info=True)
        await message.reply_text("⚠️ Произошла ошибка. Попробуйте позже.")
        return

    # Дальше — отправка в Mattermost (только если в Telegram всё успешно)
    try:
        if "активация" in final_message:
            await send_to_mm(mattermost_webhook_url=MATTERMOST_WEBHOOK_ACTIV,
                             message=final_message)
        elif "спенд" in final_message:
            await send_to_mm(mattermost_webhook_url=MATTERMOST_WEBHOOK_SPEND,
                             message=final_message)
        else:
            await send_to_mm(mattermost_webhook_url=MATTERMOST_WEBHOOK_SELL,
                             message=final_message)
    except Exception as mm_error:
        # Ошибка в MM — не критична, основное сообщение уже отправлено
        logger.warning(f"Не удалось отправить в Mattermost: {mm_error}")
        # Не прерываем поток, пользователь уже получил ответ

    # Обновляем время и очищаем данные
    last_message_time[user_id] = current_time
    del user_data[user_id]

    await message.reply_text("Сообщение отправлено.",
                             reply_markup=ReplyKeyboardRemove())
    logger.info(f"Заявка от {user_id} успешно обработана")


async def create_topic(bot: telegram.Bot, username: str):
    """
    Создаёт новый топик в группе администраторов.
    """
    try:
        response = await bot.create_forum_topic(chat_id=ADMIN_GROUP_ID,
                                                name=username)
        logger.info(
            f"Создан новый топик для {username}: {response.message_thread_id}"
        )
        return response.message_thread_id
    except telegram.error.BadRequest as e:
        logger.error(f"Не удалось создать топик для {username}: {e}")
        raise  # Передаём ошибку выше, чтобы обработать в finalize_message
    except telegram.error.Forbidden as e:
        logger.critical(f"Бот не имеет прав на создание топиков: {e}")
        raise


async def find_or_create_topic(bot: telegram.Bot, username: str, user_id: int):
    """
    Ищет топик по имени пользователя или создаёт новый, если его нет.
    """
    if str(user_id) in user_topics:
        return user_topics[str(user_id)]

    topic_id = await create_topic(bot, username)
    user_topics[str(user_id)] = topic_id
    save_user_topics(user_topics)
    return topic_id


async def handle_admin_reply(update: Update, context: CallbackContext):
    """
    Обрабатывает ответы администраторов и пересылает их обратно пользователю.
    """
    message = update.message
    reply_to_message = message.reply_to_message

    if not reply_to_message:
        return

    topic_id = reply_to_message.message_thread_id
    user_id = None

    for uid, tid in user_topics.items():
        if int(tid) == topic_id:
            user_id = int(uid)
            break

    if not user_id:
        logger.warning("Пользователь не найден.")
        return

    try:
        await context.bot.send_message(chat_id=user_id, text=message.text)
        logger.info(f"Ответ админа отправлен пользователю {user_id}")
    except telegram.error.Forbidden:
        logger.warning(f"Пользователь {user_id} заблокировал бота")
        # Можно уведомить админа, но осторожно, чтобы не спамить
    except telegram.error.BadRequest as e:
        logger.error(f"Ошибка при отправке ответа пользователю {user_id}: {e}")
    except Exception as e:
        logger.error(
            f"Непредвиденная ошибка при отправке ответа: {e}", exc_info=True
        )


def main():
    logger.info("Запуск бота...")
    # Создаем и настраиваем бота
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # Обработчики команд
    application.add_handler(CommandHandler('start', start))
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & ~filters.COMMAND,
            handle_user_message
        )
    )
    application.add_handler(
        MessageHandler(
            filters.Chat(chat_id=ADMIN_GROUP_ID) & filters.REPLY,
            handle_admin_reply
        )
    )

    logger.info("Бот успешно запущен и ожидает сообщения...")

    # Запуск бота
    application.run_polling()


def handle_exception(exc_type, exc_value, exc_traceback):
    """
    Перехватывает необработанные исключения.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("Необработанное исключение", exc_info=(exc_type, exc_value,
                                                           exc_traceback))


# Регистрируем обработчик
sys.excepthook = handle_exception


if __name__ == '__main__':
    main()
