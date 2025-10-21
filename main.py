import debugpy

"""Начальная настройка!
В BotFather с помощью команды /setprivacy, необходимо выключить "Disabled" (Выключено) Режим конфиденциальности (Privacy Mode)
Создать .env файл с одноименными параметрами TELEGRAM_TOKEN=12345, и т.д. """

import asyncio
import logging
import json
import uuid
import re
from datetime import datetime, timedelta
import time
from types import SimpleNamespace
from annotated_types import LowerCase
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import redis.asyncio as redis
from dotenv import load_dotenv
from os import getenv
import random

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command, ChatMemberUpdatedFilter, JOIN_TRANSITION, LEAVE_TRANSITION, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import ChatMemberAdministrator, ChatMemberOwner, ChatMemberRestricted, ChatMemberLeft, \
	ChatMemberBanned, ChatMemberMember, ChatPermissions, ChatMemberUpdated, InlineKeyboardMarkup, InlineKeyboardButton, \
	Message, CallbackQuery, BufferedInputFile, BotCommand
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

import html  # Для экранирования HTML-символов в именах пользователей

from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import imagehash

from google import genai
from google.genai import types as gtypes


# Настройка логирования с таймзоной UTC+10
class TzFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created) + timedelta(hours=10)
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]

# Настройка базовой конфигурации логирования
log_format = '%(asctime)s - %(levelname)s - %(message)s'
formatter = TzFormatter(log_format)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if root_logger.hasHandlers():
    root_logger.handlers.clear()
root_logger.addHandler(handler)

# Уменьшение уровня логирования для apscheduler, чтобы не спамил в лог
logging.getLogger('apscheduler').setLevel(logging.WARNING)

# Загрузка конфигурации
load_dotenv()
ADMIN_ID_STR = getenv("ADMIN_ID")
TELEGRAM_TOKEN = getenv("TELEGRAM_TOKEN")

ADMIN_ID = None
if ADMIN_ID_STR:
	try:
		ADMIN_ID = int(ADMIN_ID_STR)
	except ValueError:
		logging.error("ADMIN_ID в .env файле не является корректным числом. Функции администратора могут не работать.")

# Настройка Gemini
GOOGLE_API_KEY = getenv("GOOGLE_API_KEY")
GEMINI_MODEL = "gemini-2.5-flash" # Единая модель для всех запросов gemini-2.5-flash, "gemini-flash-latest"
gclient = genai.Client(api_key=GOOGLE_API_KEY)

REDIS_HOST = getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(getenv("REDIS_PORT", 6379))


# Конфигурация лимитов
LOCK_FOR_SUMMARIZE = set()

MAX_HISTORY = 10000  # Всего сообщений истории
MAX_TO_GPT = 2000  # Сообщений в гпт
MIN_TO_GPT = 150
MAX_SUM = 3900  # сивлолов для ответа суммар
DEF_SUM_MES = 200  # дефолтно для суммаризации
SEND_MES = 2  # число запросов в 24ч
SEND_MES_GROUP = 5  # число запросов в 24ч

BAYANDIFF = 3 # разница хешей картинок

# Конфигурация времени (единый стиль)
TIME_TO_BAN_HOURS = 1  # Время блокировки в часах
TIME_TO_BAN_SECONDS = 60 * 60 * TIME_TO_BAN_HOURS  # Время блокировки в секундах
NOTIFY_AFTER_MINUTES = 10  # Через сколько минут напоминать о проверке
NOTIFY_AFTER_SECONDS = NOTIFY_AFTER_MINUTES * 60
CLEANUP_AFTER_MINUTES = 10  # Через сколько минут удалять сообщения проверки
CLEANUP_AFTER_SECONDS = CLEANUP_AFTER_MINUTES * 60
EDIT_SEARCH_DEPTH = 200 # Глубина поиска в истории для обновления отредактированных сообщений

# --- КОНСТАНТЫ ---
SECONDS_IN_DAY = 86400
SUMMARIZE_LOCK_EXPIRY_SECONDS = 300
ADMIN_CACHE_EXPIRY_SECONDS = 60
TYPING_ACTION_INTERVAL_SECONDS = 4


# Инициализация 
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
dp["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")


# Инициализация подключения к Redis
r = None
async def init_redis():
	global r
	try:
		r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)  # Изменено
		# Проверка соединения.
		await r.ping()
		logging.info("Connected to Redis successfully")
	except Exception as e:
		logging.error(f"Failed to connect to Redis: {e}")
		# Здесь можно предусмотреть повторные попытки подключения или другие действия

async def get_admins(chat_id: int, force_refresh: bool = False) -> set:
	"""
	Возвращает множество ID администраторов чата.
	Результат кешируется в Redis на 60 секунд для уменьшения нагрузки на API.
	"""
	cache_key = f"chat:{chat_id}:admins_cache"
	
	if not force_refresh:
		cached_admins = await r.smembers(cache_key)
		if cached_admins:
			logging.debug(f"Admins for chat {chat_id} loaded from cache.")
			return {int(admin_id) for admin_id in cached_admins}

	# Если в кеше нет или нужен форс-рефреш
	logging.debug(f"Fetching admins for chat {chat_id} from Telegram API.")
	admins_list = await bot.get_chat_administrators(chat_id)
	admin_ids = {admin.user.id for admin in admins_list}
	
	# Сохраняем в кеш
	if admin_ids:
		await r.sadd(cache_key, *[str(id) for id in admin_ids])
		await r.expire(cache_key, ADMIN_CACHE_EXPIRY_SECONDS) # Кеш на 60 секунд
	return admin_ids

# Конвертер секунд
def format_seconds(seconds):
	hours = seconds // 3600
	seconds %= 3600
	minutes = seconds // 60
	if hours > 0:
		return f"{hours}ч {minutes:02d} мин."  # :02d добавляет ведущий ноль к минутам
	else:
		return f"{minutes} мин."


## Проверка настройки Privacy Mode
async def check_bot_mode():
	"""Проверяет и выводит статус Privacy Mode бота."""
	bot_info = await bot.get_me()
	if bot_info.can_read_all_group_messages:
		logging.info("Режим конфиденциальности выключен: бот может читать все сообщения в группе.")
	else:
		logging.warning("Режим конфиденциальности включен: бот имеет ограниченный доступ к чтению в группах. "
								"Пожалуйста, выключите Privacy Mode в BotFather (/setprivacy -> Disabled).")


# Начальная команда Start
@dp.message(CommandStart())
async def start(message: Message):
	"""Обработчик команды /start."""
	if message.chat.type == ChatType.PRIVATE:
		await message.answer(
			"Привет! Я бот для суммаризации чатов, проверьте мои права или права пользователя командой /right (ответьте на сообщение пользователя)."
		)


# Проверка времени работы
@dp.message(Command("run_info"))
async def run_info(message: Message, started_at: str):
	"""Обработчик команды /run_info для отображения времени запуска бота."""
	await message.answer(f"Бот запущен {started_at}")


# тест комады 
@dp.message(Command("t2"))
async def cmd2(message: Message):
	await message.answer("1")

# Новая команда для отладки
@dp.message(Command("d2"))
async def debug_message(message: Message):
	"""
	Выводит полную информацию о сообщении в лог для отладки.
	Работает только для администратора.
	Если команда вызвана в ответ на сообщение, выводится информация об отвеченном сообщении.
	"""
	if message.from_user.id != ADMIN_ID:
		await message.reply("Эта команда доступна только администратору.")
		return

	target_message = message.reply_to_message or message
	
	# Используем model_dump_json для красивого и полного вывода Pydantic модели (aiogram 3.x)
	message_info_json = target_message.model_dump_json(indent=2, exclude_none=True)
	
	logging.info(f"--- DEBUG INFO FOR MESSAGE ID: {target_message.message_id} ---")
	logging.info(message_info_json)
	
	await message.reply(f"✅ Информация о сообщении <code>{target_message.message_id}</code> выведена в лог консоли.")

# Проверка прав
@dp.message(Command("right"))
async def get_perm(message: Message):
	"""Проверяет права бота или пользователя, на сообщение которого был дан ответ."""
	chat_id = message.chat.id
	user_id_to_check = None
	user_name_to_check = None

	if message.reply_to_message:
		user_id_to_check = message.reply_to_message.from_user.id
		user_name_to_check = message.reply_to_message.from_user.full_name
		logging.info(f"Проверка прав пользователя {user_name_to_check} (ID: {user_id_to_check}) в чате: {chat_id}")
	else:
		user_id_to_check = bot.id
		check_bot_mode()
		logging.info(f"Проверка прав бота (ID: {user_id_to_check}) в чате: {chat_id}")

	has_permission, permission_message = await check_admin_permissions(chat_id, user_id_to_check)
	await message.answer(permission_message)

# Проверка прав бота/Узера
async def check_admin_permissions(chat_id, user_id_to_check):
	"""Проверяет, является ли пользователь администратором с необходимыми правами."""
	try:
		chat_member = await bot.get_chat_member(chat_id, user_id_to_check)
		user = await bot.get_chat(user_id_to_check)
		user_name = user.full_name if hasattr(user, 'full_name') else (
			user.username if hasattr(user, 'username') else f"ID: {user_id_to_check}")
		logging.info(
			f"Информация о членстве в чате для пользователя {user_name} (ID: {user_id_to_check}): {chat_member}")
		if isinstance(chat_member, ChatMemberOwner):
			return True, f"{user_name} является владельцем и имеет все разрешения."
		elif isinstance(chat_member, ChatMemberAdministrator):
			if (
					chat_member.can_delete_messages
					and chat_member.can_restrict_members
					and chat_member.can_pin_messages
					and chat_member.can_manage_topics
			):
				return True, f"{user_name} является администратором с правом удалять сообщения, блокировать участников, закреплять сообщения или управлять темами."
			else:
				return (
					False,
					f"{user_name} является администратором, но не имеет достаточных прав (нет прав на удаление, блокировку, закрепление или управление темами).",
				)
		elif isinstance(chat_member, ChatMemberRestricted):
			return False, f"{user_name} ограничен в правах и не может выполнять некоторые действия."
		elif isinstance(chat_member, ChatMemberLeft):
			return False, f"{user_name} покинул чат."
		elif isinstance(chat_member, ChatMemberBanned):
			return False, f"{user_name} заблокирован в чате."
		elif isinstance(chat_member, ChatMemberMember):
			return (
				False,
				f"{user_name} является обычным участником и не имеет административных прав.",
			)
		else:
			return False, f"Неизвестный статус для {user_name}: {type(chat_member)}"
	except Exception as e:
		logging.error(f"Ошибка при получении информации о членстве в чате: {e}")
		return False, f"Произошла ошибка при проверке прав для пользователя ID {user_id_to_check}: {e}"


# Удолятор сообщений
async def del_msg_delay(smg_obj, timer=7):
	await asyncio.sleep(timer)
	# --- УЛУЧШЕННАЯ ОБРАБОТКА ОШИБОК УДАЛЕНИЯ ---
	# Используем специфичное исключение из aiogram 3.x+
	try:
		await smg_obj.delete()
	except TelegramBadRequest as e: # Ловим более общую ошибку на случай других проблем
		# Логируем как ошибку, если это не ожидаемое "сообщение не найдено"
		logging.error(f"Не удалось удалить сообщение {smg_obj.message_id}: {e}")

# Обработка комады summarize
@dp.message(Command("sum"))
async def summarize(message: Message, command: CommandObject): # <-- Изменили сигнатуру
	logging.info(f"Команда summarize вызвана в чате: {message.chat.title}")
	if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
		await message.answer("Эта команда работает только в групповых чатах!")
		return

	user_id = message.from_user.id
	user_nm = message.from_user.username or 'БезыНЯ-шка'
	chat_id = message.chat.id
	chat_Ti = message.chat.title

	# Проверка на доступ у чата и юзера
	# req = await is_user_approved(chat_id, user_id)
	req = 'on'
	if req == 'on':
		# --- УЛУЧШЕННАЯ БЛОКИРОВКА НА REDIS ---
		lock_key = f"chat:{chat_id}:sum_lock"
		# Пытаемся установить блокировку на 5 минут. Если ключ уже есть, вернется False.
		is_lock_acquired = await r.set(lock_key, 1, ex=300, nx=True)
		
		if not is_lock_acquired:
			await message.reply("Уже обрабатываю предыдущий запрос на сводку в этом чате. Пожалуйста, подождите.")
			return

		# --- НОВАЯ, УЛУЧШЕННАЯ ЛОГИКА ПАРСИНГА ---
		args = command.args.split() if command.args else []
		
		# Отладочная команда для снятия блокировки
		if 'unlock' in args and user_id == ADMIN_ID:
			await r.delete(lock_key)
			await message.reply("Блокировка снята.")
			return

		# Ищем флаг приватности. Он может быть '-p' или 'private'.
		is_private = '-p' in args or 'private' in args
		
		# Отфильтровываем все флаги, оставляя только числовые параметры
		numeric_params = [p for p in args if p not in ['-p', 'private']]
		
		num_messages = 0
		offset = 0

		try:
			if len(numeric_params) >= 1:
				num_messages = int(numeric_params[0])
				num_messages = max(MIN_TO_GPT, min(num_messages, MAX_TO_GPT))
			if len(numeric_params) >= 2:
				offset = int(numeric_params[1])

			# Передаем новый флаг в вашу основную функцию
			await process_summarize(message, num_messages, offset, privat=is_private)

		except ValueError:
			await del_msg_delay(await message.answer(f"Некорректное число сообщений. Использую {DEF_SUM_MES}."))
			await process_summarize(message, DEF_SUM_MES)
		finally:
			# Снимаем блокировку, только если мы ее установили
			await r.delete(lock_key)

	elif req == 'off':
		if not ADMIN_ID:
			logging.error("ADMIN_ID не установлен в .env, не могу отправить запрос на одобрение.")
			await message.reply("Функция требует настройки администратором бота.")
			return

		request_id = await create_approval_request(user_id, chat_id, chat_Ti, user_nm)
		keyboard = InlineKeyboardMarkup(inline_keyboard=[
			[InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{request_id}:{chat_id}")],
			[InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{request_id}:{chat_id}")]
		])

		await bot.send_message(
			ADMIN_ID,
			f"⚠️ Новый запрос на доступ:\n"
			f"User: {message.from_user.full_name} (@{user_nm})\n"
			f"Группа: {chat_Ti}\n"
			f"ID запроса: {request_id}",
			reply_markup=keyboard
		)

		await del_msg_delay(await message.reply(
			f"❌ @{user_nm} Вам пока не доступна эта функция.\n"
			"Запрос на одобрение отправлен администратору."
		))

	else:
		await del_msg_delay(await message.answer(f"⚠️ @{user_nm} Ожидайте ответа по запросу {req}."))


# чекаем в бд sum_access
async def is_user_approved(chat_id: int, user_id: int) -> str:
	mes = json.loads(await r.hget(f"chat:{chat_id}:sum_access", user_id) or "{}")	
	if not mes or mes['access'] == 0:
		return 'off'
	elif mes['access'] == 1:
		return 'on'
	else:
		return mes['access']
	 

# на всякий случий, автосчетчик максимального допустимого числа знаков для ответа
def auto_gpt_mes_count(value):
	"""Интерполяция с быстрым ростом при малых значениях.
	Возвращает примерное количество символов для запроса к GPT в зависимости от числа сообщений.
	"""
	if value < 100:
		return 500
	elif value > 2000:
		return 3700
	else:
		# Нелинейная интерполяция для более быстрого роста при малых значениях
		return int(500 + (value - 100) * (3700 - 500) / (2000 - 100) * (0.8 * (1 - (value - 100) / 500) ** 2 + 0.2))

async def _fetch_messages_for_summary(chat_id: int, count: int, start: int) -> tuple[list, int, str | None]:
	"""
	Извлекает сообщения из Redis для суммаризации.
	Возвращает (список сообщений, количество, ID последнего сообщения для ссылки).
	"""
	key = f"chat:{chat_id}:history"
	last_sum_id = None
	
	if count > 0:
		# Пользовательский запрос с указанием количества
		logging.info(f'Запрос на сводку {count} сообщений со смещением {start}')
		messages_json = await r.lrange(key, start, start + count - 1)
		messages = [json.loads(m) for m in messages_json]
		messages.reverse()
	else:
		# Автоматический запрос (новые сообщения с последней сводки)
		last_sum_data = await r.hgetall(f"chat:{chat_id}:last_sum")
		msg_old_id = int(last_sum_data.get('id', 0))
		last_sum_id = last_sum_data.get('msg_id')

		# Если последней сводки не было, берем DEF_SUM_MES
		if msg_old_id == 0:
			messages_json = await r.lrange(key, 0, DEF_SUM_MES - 1)
			messages = [json.loads(m) for m in messages_json]
			messages.reverse()
		else:
			messages = []
			history_chunk = await r.lrange(key, 0, MAX_TO_GPT) 
			for msg_json in history_chunk:
				msg = json.loads(msg_json)
				if msg['id'] > msg_old_id:
					messages.append(msg)
				else:
					break # Нашли границу, прекращаем
			messages.reverse() # Восстанавливаем хронологический порядок
	return messages, len(messages), last_sum_id

# Делаем суммаризацию
async def process_summarize(message: Message, count=0, start=0, privat: bool = False):
	chat = message.chat
	chat_id = chat.id
	user = message.from_user

	# <--- 2. Определяем, куда отправлять ответ
	if privat:
		target_chat_id = user.id
	else:
		target_chat_id = chat.id
	
	ttl = await check_daily_limit(chat_id, user.id)
	if (ttl > 0):
		await del_msg_delay(await message.answer(f"❌ Достигнут лимит запросов суммаризации!\n Подождите {format_seconds(ttl)}"))
		return
	
	messages, final_count, last_sum_id = await _fetch_messages_for_summary(chat_id, count, start)

	if not messages:
		await del_msg_delay(await message.answer("Нет сообщений для суммаризации."))
		return

	chat_id_str = str(chat_id)[4:]
	turl = f"t.me/{chat.username}/" if chat.username else f"t.me/c/{chat_id_str}/"
	if last_sum_id: 
		surl = f'Предыдущий свод [тут]({turl}{last_sum_id})' # Используем стандартный Markdown
	else:
		surl = ''

	typing_task = None # Инициализируем переменную для задачи
	try:
		# 1. Создаем и запускаем фоновую задачу, которая будет слать "typing"
		async def send_typing_periodically():
			"""Отправляет 'typing' каждые 4 секунды, пока не будет отменена."""
			while True:
				await bot.send_chat_action(target_chat_id, action="typing") # Отправляем "typing"
				await asyncio.sleep(4) # Пауза 4 секунды (безопасно меньше лимита Telegram)

		typing_task = asyncio.create_task(send_typing_periodically())

		logging.info(f"Передача {final_count} сообщений в AI для чата {chat_id}")
		summary = await get_gpt4_summary(messages, turl)

		if not summary:
			logging.error("AI returned an empty or None summary, cannot proceed.")
			await bot.send_message(target_chat_id, "Не удалось сгенерировать сводку. Попробуйте позже.")
			return # Выходим, finally выполнится

		logging.info(f"Ответ gpt4: получен. Длина {len(summary)}")
		await r.lpush('gpt_answ_t', json.dumps(summary))
		await r.ltrim('gpt_answ_t', 0, 14) # Храним последние 15 записей

		if typing_task:
			typing_task.cancel()

		# --- НОВАЯ ЛОГИКА: ПОВТОРНАЯ СУММАРИЗАЦИЯ, ЕСЛИ ТЕКСТ СЛИШКОМ ДЛИННЫЙ ---
		if len(summary) > MAX_SUM:
			logging.warning(f"Сводка слишком длинная ({len(summary)} > {MAX_SUM}). Запускаю повторное сокращение.")
			
			# Снова включаем "typing", чтобы пользователь видел, что работа продолжается
			typing_task = asyncio.create_task(send_typing_periodically())

			try:
				# Вызываем новую функцию для сокращения
				shortened_summary = await shorten_text_with_ai(summary)
				
				# Используем сокращенную версию, только если она действительно короче
				if shortened_summary and len(shortened_summary) < len(summary):
					logging.info(f"Текст успешно сокращен до {len(shortened_summary)} символов.")
					summary = shortened_summary
				else:
					logging.warning("Не удалось сократить текст или сокращенный текст длиннее оригинала. Используется исходный.")
			except Exception as e:
				logging.error(f"Ошибка при повторном сокращении: {e}")
				# В случае ошибки просто продолжаем с исходным длинным текстом, Telegram его обрежет.
			finally:
				# Убедимся, что задача "typing" будет отменена
				if typing_task:
					typing_task.cancel()
		
		# <--- 4. Используем bot.send_message для отправки в target_chat_id
		sum_text = f"📝 \#Суммаризация последних {final_count} сообщений:\n{summary}"
		if surl and not privat: # Добавляем ссылку на пред. свод только если постим в чат
			sum_text += f"\n{surl}"

		sum = await bot.send_message(
			chat_id=target_chat_id,
			text=sum_text,
			disable_web_page_preview=True,
			parse_mode="MarkdownV2"
		)
	# <--- 5. Обработка ошибки, если бот не может написать в личку
	except TelegramForbiddenError:
		logging.error(f"Не удалось отправить сообщение пользователю {user.id}. Бот заблокирован или чат не начат.")
		await bot.send_message(target_chat_id, "Не всё так просто, спроси сам знаешь кого ))")
		return
	except Exception as e:
		if "can't parse entities" in str(e).lower():
			logging.error(f"Markdown parse error, sending plain text. Error: {e}")
			await bot.send_message(target_chat_id, sum_text, disable_web_page_preview=True)
			return
		logging.error(f"Error in process_summarize: {e}", exc_info=True)
		await bot.send_message(target_chat_id, "Ошибка получения данных от AI или отправки сообщения.")
		return
	finally:
		if typing_task:
			typing_task.cancel()

	await upd_daily_limit(chat_id, user.id, privat)
	
	# Обновляем last_sum только если это не приватный запрос и не кастомный по числу
	if not privat and count == 0:
		key = f"chat:{chat_id}:last_sum"
		async with r.pipeline() as pipe:
			pipe.hset(key, 'id', messages[-1]['id']) # ID последнего обработанного сообщения
			pipe.hset(key, 'msg_id', sum.message_id)
			pipe.hset(key, 'text', json.dumps(summary))
			pipe.lpush(key+":all",json.dumps([messages[-1]['id'],sum.message_id,messages,summary]))
			pipe.ltrim(key+":all", 0, 10 - 1)
			await pipe.execute()	


# ставим в бд status pending
async def create_approval_request(user_id: int, chat_id: int, chat_Ti: str, user_nm: str) -> str:
	request_id = str(uuid.uuid4())
	request_data = {
		"user_id": user_id,
		"user_nm": user_nm,
		"chat_id": chat_id,
		"chat_Ti": chat_Ti,
		"timestamp": datetime.now().isoformat(),
		"status": "pending"
	}
	async with r.pipeline() as pipe:
		pipe.hset(f"chat:{chat_id}:sum_request", request_id, json.dumps(request_data))
		pipe.hexpire(f"chat:{chat_id}:sum_request", 3*86400, request_id)
		pipe.hset(f"chat:{chat_id}:sum_access", user_id, json.dumps({'access': request_id}))
		pipe.hexpire(f"chat:{chat_id}:sum_access", 3*86400, user_id)
		await pipe.execute()
	return request_id


# чекаем в бд дневной лимиты
async def check_daily_limit(chat_id: int, user_id: int) -> int:
	key = f"chat:{chat_id}:sum_limits"
	user_id_str = str(user_id)

	# Используем HMGET для получения значений и HTTL для TTL в одном пайплайне
	async with r.pipeline() as pipe:
		pipe.ttl(f"{key}:{user_id_str}")
		pipe.ttl(f"{key}:group")
		user_ttl, group_ttl = await pipe.execute()

	return max(user_ttl or 0, group_ttl or 0)


# Обновляем лимиты
async def upd_daily_limit(chat_id: int, user_id: int, privat:bool):
	key = f"chat:{chat_id}:sum_limits"
	group_field = "group"

	# Используем отдельные ключи для TTL, это проще и надежнее
	user_key = f"{key}:{user_id}"
	group_key = f"{key}:{group_field}"

	async with r.pipeline() as pipe:
		pipe.incr(user_key)
		pipe.expire(user_key, SECONDS_IN_DAY // SEND_MES)
		
		if not privat:
			pipe.incr(group_key)
			pipe.expire(group_key, SECONDS_IN_DAY // SEND_MES_GROUP)
		
		await pipe.execute()


async def shorten_text_with_ai(text: str) -> str | None:
	"""Сокращает уже сгенерированный текст, если он слишком длинный."""
	logging.info(f"Попытка сократить текст длиной {len(text)} до {MAX_SUM} символов.")
	prompt = f"""
**Задача:** Сократи этот текст до {MAX_SUM} символов.

**Правила:**
1.  **Сохрани суть:** Главные темы, выводы и шутки должны остаться.
2.  **Сохрани MarkdownV2:** Всё форматирование (`*жирный*`, `_курсив_`), а главное — все ссылки вида `текст` должны остаться в неизменном виде. Не меняй текст внутри ссылок и сам URL.
3.  **Не добавляй ничего от себя:** Никаких комментариев, только сокращенный текст.
4.  **Ответ должен быть строго короче {MAX_SUM} символов.**

**Текст для сокращения:**
{text}
"""
	try:
		response = await gclient.aio.models.generate_content(
			model=GEMINI_MODEL,
			contents=prompt,
		)
		
		# Дополнительная проверка, что ответ не пустой
		if not response.text or not response.text.strip():
			logging.error("Модель для сокращения вернула пустой ответ.")
			return text

		# Преобразуем Markdown и возвращаем
		return markdown_to_tg_v2(response.text)

	except Exception as e:
		logging.error(f"Error in shorten_text_with_ai: {e}", exc_info=True)
		return text # В случае ошибки возвращаем исходный текст, чтобы не потерять сводку

# Запрос к ИИ
async def get_gpt4_summary(text: list, turl: str) -> str | None:
	#return f"Jlsdgssdgdfhdh\n"	
	# MAX_SUM = auto_gpt_mes_count(count)

	# Задаем системную инструкцию при создании модели
	#первая строка должна быть такой: `--- cut here ---` будет служить для "отрезки лишнего"
	#turl = "t.me/chatname/"
	prompt = f"""
**Роль:** Ты — AI-ассистент, который создаёт краткие и остроумные пересказы диалогов из Telegram-чатов в формате **MarkdownV2**, готовом для отправки в Telegram.

**Задача:** Проанализируй предоставленный фрагмент чата в формате JSON и создай на его основе пост для Telegram, следуя правилам ниже.

**Форматирование**
1. Используй **строго** MarkdownV2: `*жирный*`, `_курсив_`, `> цитаты`, списки с `•`.

**Контекст и входные данные:**
В твоем распоряжении базовая ссылка на чат и json строки :
1.  {turl} — базовая ссылка на чат.
2.  json строки: массив сообщений, где каждый объект содержит `id` сообщения, `reply_to` ответ на сообщение (может не быть), `full_name`, `user_name` (может быть `None`) и `text`.

**Пример структуры входных данных:**
```json
(
  "id": 455000,
  "user_name": "None",
  "full_name": "Valery Gordienko",
  "text": "Это не одиночный, здесь предохранитель есть."
)
(
  "id": 455001,
  "reply_to": 454999,
  "user_name": "karamba666",
  "full_name": "Павел",
  "text": "В заднем кармане 🌚"
)
(
  "id": 455028,
  "user_name": "AndzhelaA78",
  "full_name": "Анджела Аргунова",
  "text": "Доброе утро"
)```

**ТРЕБОВАНИЯ К ОТЧЁТУ (СЛУШАЙ ВНИМАТЕЛЬНО!):**

Твой пересказ — это крутой пост в TG.

1.  **Заголовок с эмодзи.** Никаких "Названий темы". Вместо этого — **жирный заголовок и 1-3 подходящих по смыслу эмодзи**.

2.  **Никаких списков "Участники".** Сразу после заголовка — сочный пересказ на 3-7 предложений, *кратко описывающий общую атмосферу и ключевые темы чата*.

3.  **Имена и ссылки на юзеров.**

	  * Используй только имя из поля `full_name` (например, из "Valery Gordienko" делай *Валерий*, из "Анджела Аргунова" — *Анджела*). Следи за правильностью имён, не допускай опечаток (например, пиши "Анджела", а не "Анжела", сверяясь с `full_name`).
	  * При **первом** упоминании пользователя в сводке сделай его имя кликабельной ссылкой, если у него есть `user_name`. Формат: `[Имя](t.me/user_name)`.
	  * Если `user_name` равен `None`, имя пишется просто текстом (можно *жирным*), без ссылки. Например, для "Vasili Petrovich" с `user_name: None` ты напишешь просто *Василий*.
	  * При **повторных** упоминаниях этого же пользователя в этой же сводке ссылка больше **не нужна**. Имя можно выделить *жирным* для акцента.
	  * Экранируй спецсимволы в именах, к примеру `Вас_Я` приводи к `ВасЯ` или `Вас-Я`.
	  * Пример: ...а *Иван* в ... или ... попросили *Романа* сделать ... или ... упомянув *Лену* в ...
4.  **Ссылки на сообщения.**

	  * Вместо скучных `[id:...]` делай живую ссылку прямо из ключевой фразы в тексте (1-3 слова). Текст ссылки должен быть экранирован от спецсимволов MarkdownV2.
	  * Формат ссылки: `ключевая фраза`. Переменные `id` бери из входного JSON. Никаких разрывов между `](`.
	  * Пример: ...опять сидели бы без [горячей воды](t.me/chatname/452339) в ... или ...пустили новый [слух о ремонте](t.me/chatname/452345) дорог...

5.  **Тон — наше всё!** Пиши так, будто рассказываешь другу, что он пропустил. Используй иронию, подколки, эмодзи и восклицательные знаки, чтобы текст был живым и энергичным.

6.  **Содержание сводки.**

	  * **Фокусируйся на ключевых темах, идеях и выводах.** Избегай дословного пересказа мелких реплик, флуда и шуток, не влияющих на суть.
	  * **Группируй связанные по смыслу сообщения** в один логический блок, даже если они были написаны не по порядку.

7.  **Пределы.**

	  * **Максимальная длина ответа — {MAX_SUM} символов.** Но стремись к максимальной сжатости без потери смысла. _Less is more_.

**И ОБЯЗАТЕЛЬНО ПОВТОРНО ПРОВЕРЬ перед отправкой:**
1. Длину пересказа (7й пункт требований)
2. Корректность и видимость ссылок (4й пункт требований) и правильность экранирования для MarkdownV2."""
	try:
		response = await gclient.aio.models.generate_content(
			model=GEMINI_MODEL,
			config=gtypes.GenerateContentConfig(
				tools=[
					gtypes.Tool(url_context=gtypes.UrlContext()),
				],
				thinking_config=gtypes.ThinkingConfig(thinking_budget=11000),
				system_instruction=prompt,  # Системная инструкция
				),
			contents=json.dumps(text),  # Описание, текст в формате JSON
			)

		# Сохраняем успешный ответ для отладки
		try:
			debug_info = {
				"timestamp": datetime.now().isoformat(),
				"status": "success",
				"input_messages_count": len(text),
				"response_text": response.text,
				"prompt_feedback": str(response.prompt_feedback) if response.prompt_feedback else "N/A",
			}
			await r.lpush("For_debag", json.dumps(debug_info, ensure_ascii=False))
			await r.ltrim("For_debag", 0, 15) # Храним последние 15 записей
		except Exception as redis_e:
			logging.error(f"Redis error in get_gpt4_summary (debug log): {redis_e}")

		return markdown_to_tg_v2(response.text)

	except Exception as e:
		logging.error(f"Error in get_gpt4_summary while calling Gemini: {e}", exc_info=True)
		# Сохраняем информацию об ошибке для отладки
		try:
			error_info = {
				"timestamp": datetime.now().isoformat(),
				"status": "error",
				"error_message": str(e),
				"input_messages_count": len(text),
			}
			await r.lpush("For_debag", json.dumps(error_info, ensure_ascii=False))
			await r.ltrim("For_debag", 0, 10)
		except Exception as redis_e:
			logging.error(f"Redis error in get_gpt4_summary (error log): {redis_e}")
		return None

# Обработка подтверждения/отказа запроса
@dp.callback_query(F.data.startswith("approve:") | F.data.startswith("reject:"))
async def handle_approval(callback: CallbackQuery):
	logging.info(f"approval callback_query {callback.data}")
	action, request_id, chat_id = callback.data.split(":")

	request_data = json.loads(await r.hget(f"chat:{chat_id}:sum_request", request_id) or "{}")
	uname = (f"@{request_data['user_nm']}")

	if not request_data:
		return await callback.answer("Запрос не найден!")

	if action == "approve":
		await r.hset(f"chat:{chat_id}:sum_access", request_data['user_id'],
					 json.dumps({'access': 1, 'request_id': request_id}))
		await r.hpersist(f"chat:{chat_id}:sum_access", request_data['user_id'])
		#await r.hset(f"chat:{chat_id}:sum_request", request_id, json.dumps({'status': 'approved'}))

		await callback.message.edit_text(f"✅ Запрос {request_id} для {uname} одобрен!")

		await del_msg_delay(await bot.send_message(
			chat_id,
			f"🎉 {uname} Вам одобрен доступ к команде /summarize !"
		))
	else:
		await r.hset(f"chat:{chat_id}:sum_access", request_data['user_id'],
					 json.dumps({'access': 0, 'request_id': request_id}))
		await r.hpersist(f"chat:{chat_id}:sum_access", request_data['user_id'])

		await callback.message.edit_text(f"❌ Запрос {request_id} для {uname} отклонен!")

		await del_msg_delay(await bot.send_message(
			request_data['user_id'],
			f"🚫 {uname} Ваш запрос на доступ к /summarize был отклонен."
		))

	await callback.answer()


# Команда для добавления хеша в белый список
@dp.message(Command("bw"))
async def whitelist_bayan(message: Message):
	"""
	Добавляет хеш изображения/превью из отвеченного сообщения в белый список баянов.
	"""
	try:
		if message.from_user.id not in await get_admins(message.chat.id):
			await del_msg_delay(await message.reply("Эта команда доступна только администраторам."))
			return

		if not message.reply_to_message:
			await del_msg_delay(await message.reply("Нужно ответить на сообщение с картинкой или видео."))
			return

		reply = message.reply_to_message
		file_id = None
		if reply.photo:
			file_id = reply.photo[-1].file_id # Берем самое качественное
		elif reply.video and reply.video.thumbnail:
			file_id = reply.video.thumbnail.file_id

		if not file_id:
			await del_msg_delay(await message.reply("В сообщении не найдено подходящей картинки или превью."))
			return

		hash_val = await get_image_hash(file_id)
		if not hash_val:
			await del_msg_delay(await message.reply("Не удалось получить хеш изображения."))
			return

		key = f"chat:{message.chat.id}:bayan_whitelist"
		is_new = await r.hset(key, hash_val, 1) # Используем 1 как простое значение-флаг
		if is_new:
			await del_msg_delay(await message.reply(f"✅ Хеш <code>{hash_val}</code> добавлен в белый список баянов."))
		else:
			await del_msg_delay(await message.reply(f"ℹ️ Хеш <code>{hash_val}</code> уже был в белом списке."))
	finally:
		try:
			await message.delete()  # Удаляем команду в любом случае
		except Exception as e:
			# Логируем, но не падаем, если не удалось удалить
			logging.warning(f"Не удалось удалить команду /bw от {message.from_user.id} в чате {message.chat.id}: {e}")

########## Чекаем баяны ########
async def get_image_hash(file_id: str) -> str | None:
	"""Получает dHash изображения по file_id. Возвращает None в случае ошибки."""
	try:
		# Получаем информацию о файле
		file = await bot.get_file(file_id)
		# Скачиваем файл как bytes
		file_bytes = await bot.download_file(file.file_path)
		file_bytes.seek(0)  # Сбрасываем указатель в начало
		
		# Открываем изображение напрямую из file_bytes
		img = Image.open(file_bytes)		
		hash_val = imagehash.dhash(img, hash_size=8)
		# Закрываем BytesIO
		file_bytes.close()
		return str(hash_val)
	except Exception as e:
		# Не падать, просто логировать и вернуть None
		logging.error(f"Error getting image hash in get_image_hash for file_id {file_id}: {e}")
		return None
# Функция для вычисления расстояния Хэмминга
async def hamming_distance(hash1, hash2):
	if len(hash1) != len(hash2):
		raise ValueError("Хэши должны быть одинаковой длины")
	return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

# Функция для поиска похожих хэшей в хэш-таблице Redis
async def find_similar_hash_in_hset(new_hash: str, redis_key: str, max_diff: int) -> tuple[str, str] | None:
	"""
	Ищет похожий хэш в хэш-таблице Redis.
	Сначала ищет точное совпадение, затем похожие.
	Возвращает кортеж (найденный_хеш, значение) или None.
	"""
	# 1. Проверка на точное совпадение для производительности
	exact_match_value = await r.hget(redis_key, new_hash)
	if exact_match_value:
		return new_hash, exact_match_value

	# 2. Если точного совпадения нет, ищем похожие
	all_hashes = await r.hkeys(redis_key)
	for saved_hash in all_hashes:
		if await hamming_distance(new_hash, saved_hash) <= max_diff:
			value = await r.hget(redis_key, saved_hash)
			return saved_hash, value # Возвращаем кортеж (хеш, значение)
	return None

# Функция для проверки, является ли сообщение баяном
async def check_bayan(message: Message):
	key = f"chat:{message.chat.id}:bayan"
	whitelist_key = f"chat:{message.chat.id}:bayan_whitelist"
	chat_id = message.chat.id
	message_id = message.message_id
	bayan = False
	file_id = None

	if message.photo:
		file_id = message.photo[-1].file_id # Берем самое качественное для точности
	elif message.video and message.video.thumbnail:
		file_id = message.video.thumbnail.file_id
	
	if not file_id:
		return bayan

	# Получаем хеш нового изображения
	new_hash = await get_image_hash(file_id)

	if not new_hash:
		return bayan # Не удалось создать хеш, считаем не баяном

	# Проверяем, есть ли похожий хеш в белом списке
	if await find_similar_hash_in_hset(new_hash, whitelist_key, max_diff=BAYANDIFF):
		logging.info(f"Хеш {new_hash} (или похожий) в белом списке для чата {chat_id}. Проверка на баян пропущена.")
		return bayan

	# Ищем похожий хеш в истории баянов
	bayan_match = await find_similar_hash_in_hset(new_hash, key, max_diff=BAYANDIFF)
	if bayan_match:
		saved_hash, bayan_value_json = bayan_match
		original_message_id = json.loads(bayan_value_json)['id']

		# --- Проверяем, существует ли исходное сообщение ---
		try:
			if not ADMIN_ID:
				logging.warning("ADMIN_ID не установлен. Проверка существования баяна пропускается.")
				raise ValueError("Admin ID not set")

			# Копируем сообщение в чат с админом, чтобы проверить его существование.
			# Сразу удаляем копию, чтобы не беспокоить.
			copied_message_id_obj = await bot.copy_message(
				chat_id=ADMIN_ID,
				from_chat_id=chat_id,
				message_id=original_message_id,
				disable_notification=True
			)
			# Удаляем скопированное сообщение, используя его ID
			await bot.delete_message(chat_id=ADMIN_ID, message_id=copied_message_id_obj.message_id)

			# --- Сообщение существует, это баян ---
			id = original_message_id
			gif_list = (['CgACAgIAAxkBAAIDDmf9qNvELAABV1bhizjsVWVdg_oDYwACjQAD24TBAwNGZWxV-v8LNgQ'], # баба кидает
						['CgACAgQAAxkBAAIDD2f9xJ5wVliPMOyIkLBYFIjVyckiAALEOQAC7hdkB6taFHfHHCwtNgQ'], # в сене находит
						['CgACAgQAAxkBAAIDEGf9xrNV-0FR9CwXnRTzR9as3lOyAALpAgACdgQUUx8P27eBaYgLNgQ'], # Джери
						['CgACAgQAAxkBAAIDEWf9x0Z0QSCD0eWCudKndoLwIHaTAAJSAwAC-RAEU2Tw7_4c3dtnNgQ'], # Пингвин
						['CgACAgIAAyEFAASJUrtlAAII3Gj3T4LlBDxgEqzif7KBQVOQpCLSAAJ2AAPD8JlIrbepiRS69Qk2BA'], # Круз
						['CgACAgQAAxkBAAIDGmf9z3yk_q374r08VTF5MVgqE0UgAAIlAwAClTENU85HwIjkanSyNgQ']) # Мужик
			gif_id = (random.choice(gif_list))[0]
			# Для приватной супергруппы убираем префикс "-100"
			chat_id_str = str(chat_id)[4:]        # '1473943182'
			if message.chat.username:
				turl = f"t.me/{message.chat.username}/{id}"
			else:
				turl = f"t.me/c/{chat_id_str}/{id}"
			
			bayan = True
			try:
				await message.reply_animation(animation=gif_id, caption=f"Архивная запись №{id}!   [Оригинал тут]({turl})", parse_mode="Markdown")
			except TelegramBadRequest as e:
				logging.error(f"Error replying with animation in check_bayan: {e}")

		except TelegramBadRequest as e:
			if "message to copy not found" in e.message:
				# --- Исходное сообщение удалено, обновляем базу ---
				logging.info(f"Исходный баян (msg_id: {original_message_id}) удален. Обновляем запись в Redis.")
				await r.hdel(key, saved_hash) # Удаляем старый хеш
				await r.hset(key, new_hash, json.dumps({'id': message_id})) # Добавляем новый
				bayan = False # Это уже не баян
			else:
				logging.error(f"Telegram API error in check_bayan: {e}")
				bayan = False
		except Exception as e:
			logging.error(f"Unexpected error in check_bayan for message {message.message_id}: {e}", exc_info=True)
			bayan = False
	else:
		await r.hset(key, new_hash, json.dumps({'id': message_id}))
	return bayan

############# Анекдот для Даси ##########
@dp.message(Command("анекдот"))
async def send_fun_mes(message: Message):
	key1 = f"chat:{message.chat.id}:gemini"
	tiktak = await r.ttl(key1)
	if tiktak > 0:
		await message.answer(f"Нужно пододжать менее {tiktak // 60 + 1} мин")
		return
	
	tema = message.text[8:]
	if tema:
		contents = f"Придумай короткий и очень смешной анекдот для русской души на тему: {tema}"
	else:
		key = f"chat:{message.chat.id}:history"
		txt = await r.lrange(key, 0, 30)
		txt = [json.loads(message_json)['text'] for message_json in txt]
		txt.reverse()
		contents = f"Придумай короткий и очень смешной анекдот для русской души по мотивам данной переписки: {txt}"	

	# Генерируем контент асинхронно
	response = await gclient.aio.models.generate_content(
		model=GEMINI_MODEL,
		contents=contents,
	)
	await message.answer(response.text or "Всё плохо")
	await r.set(key1, 'gemini', ex=300)

############# Рисуем ТОП ##########
async def generate_pil_image(top_users, title):
	# Размеры изображения
	width = 600
	row_height = 30
	header_height = 40
	padding = 20
	row_spacing = 10
	max_rows = len(top_users)

	# Создаем новое изображение с белым фоном
	img = Image.new('RGB', (width, header_height + padding + max_rows * (row_height + row_spacing)), color=(255, 255, 255))
	draw = ImageDraw.Draw(img)

	# Загружаем шрифт (можно указать путь к вашему шрифту)
	try:
		font = ImageFont.truetype("Roboto-Bold.ttf", 20)  # Попробуем Roboto Bold
		font_regular = ImageFont.truetype("Roboto-Regular.ttf", 16)
	except OSError:
		font = ImageFont.load_default()  # Если не найдено, используем шрифт по умолчанию
		font_regular = ImageFont.load_default()

	# Рисуем заголовок
	draw.text((width / 2, padding + 10), title, font=font, fill=(0, 0, 0), anchor="mm")

	# Рисуем строки с данными
	y = header_height + padding
	for i, (id, value, name) in enumerate(top_users):
		# display_value = humanized_value if humanized_value else str(value)
		display_value = str(value)
		bg_color = (240, 240, 240) if i % 2 == 0 else (248, 248, 248)  # Чередующийся фон строк
		draw.rectangle((0, y, width, y + row_height), fill=bg_color) # Изменено

		draw.text((padding, y + row_height / 2), f"{i + 1}. {name}", font=font_regular, fill=(0, 0, 0), anchor="lm")
		# Calculate the x-position for the value,留出足够的空间
		value_x = width - padding - 50
		draw.text((value_x, y + row_height / 2), display_value, font=font_regular, fill=(0, 0, 0), anchor="rm")
		y += row_height + row_spacing

	# Сохраняем изображение в BytesIO
	output = BytesIO()
	img.save(output, format='PNG')
	output.seek(0)  # Перемещаем указатель в начало
	return output

# вывод топа пользователей
@dp.message(Command("top_u"))
async def get_top_users(message: Message, command: CommandObject) -> list:
	chat_id = message.chat.id	
	
	# --- 1. Гибкий парсинг аргументов ---
	args = command.args.split() if command.args else []

	# Значения по умолчанию
	stat_type = 'm'
	count = 10
	output_type = 't'
	period = 'all_time'
	
	# Перебираем аргументы и определяем их по формату
	for arg in args:
		if arg in ['m', 'c', 'b', 'e']:
			stat_type = arg
		elif arg.isdigit():
			count = int(arg)
		elif arg in ['p', 't']:
			output_type = arg
		elif re.match(r"^\d{4}-\d{2}$", arg) or arg == 'all':
			period = arg

	# --- 2. Обновленный мануал ---
	if not command.args:
		manual = (
			"📋 <b>Команда /top_u</b>\n"
			"Показывает топ пользователей по активности в чате.\n\n"
			"<b>Формат:</b> /top_u [тип] [число] [период] [вывод]\n"
			"Аргументы можно указывать в любом порядке.\n\n"
			"<b>Типы топа:</b>\n"
			"  `m` - по сообщениям (по умолч.)\n"
			"  `c` - по символам\n"
			"  `b` - по баянам\n"
			"  `e` - по эффективности (спам-рейтинг)\n"
			"<b>Число:</b> кол-во юзеров в топе (по умолч. 10)\n"
			"<b>Период:</b> `ГГГГ-ММ` (напр. `2025-07`) или `all` (за всё время, по умолч.)\n"
			"<b>Вывод:</b> `t` - текст (по умолч.), `p` - картинка\n\n"
			"<b>Примеры:</b>\n"
			"`/top_u m 5` - топ-5 по сообщениям за всё время\n"
			"`/top_u e 10 p 2024-06` - спам-топ за июнь картинкой"
		)
		await message.answer(manual, parse_mode='HTML')
		return

	# --- 3. Динамическое формирование ключей и заголовков ---
	period_str = "всё время" if period == 'all_time' else f"период {period}"
	
	type_map = {
		'm': 'msg',
		'c': 'len',
		'b': 'byn'
	}
	title_map = {
		'm': f"Топ {count} по сообщениям за {period_str}",
		'c': f"Топ {count} по символам за {period_str}",
		'b': f"Топ {count} баянистов за {period_str}",
		'e': f"Топ {count} по эффективности за {period_str} (спам-рейтинг)"
	}

	title = title_map.get(stat_type)
	result = []

	# --- 4. Логика получения данных ---
	if stat_type in ['m', 'c', 'b']:
		redis_key_suffix = type_map[stat_type]
		key = f"chat:{chat_id}:count_u_{redis_key_suffix}:{period}"
		
		top_users_raw = await r.zrevrange(key, 0, count - 1, withscores=True)
		for user_id_bytes, score in top_users_raw:
			score = int(score)
			try:
				member = await bot.get_chat_member(chat_id, int(user_id_bytes))
				name = member.user.full_name or "No_Name"
			except Exception:
				name = f"ID:{user_id_bytes}" # Пользователь мог покинуть чат
			result.append((user_id_bytes, score, name))

	elif stat_type == 'e':
		key_msg = f"chat:{chat_id}:count_u_msg:{period}"
		key_len = f"chat:{chat_id}:count_u_len:{period}"
		
		user_msg_counts = await r.zrange(key_msg, 0, -1, withscores=True)
		efficiency_list = []
		MIN_MESSAGES = 10  # Минимальное кол-во сообщений для учета

		for user_id_bytes, msg_count in user_msg_counts:
			if msg_count < MIN_MESSAGES:
				continue
			
			char_count = await r.zscore(key_len, user_id_bytes)
			if char_count is not None and msg_count > 0:
				efficiency = float(char_count) / msg_count
				efficiency_list.append((user_id_bytes, efficiency))

		efficiency_list.sort(key=lambda x: x[1]) # Сортируем по возрастанию
		top_efficiency = efficiency_list[:count]

		for user_id, efficiency_score, in top_efficiency:
			try:
				member = await bot.get_chat_member(chat_id, int(user_id))
				name = member.user.full_name or "No_Name"
			except Exception:
				name = f"ID:{user_id}"
			result.append((user_id, efficiency_score, name))

	if not result:
		await message.answer(f"Нет данных для статистики за `{period}`. Возможно, в этот период не было активности.")
		return

	# --- 5. Логика вывода (остается без изменений) ---
	if output_type == 't':
		MAX_LEN, MAX_PLACE_LEN, MAX_SCORE_LEN = 30, 2, 6
		MAX_DISPLAY_NAME_LEN = MAX_LEN - MAX_PLACE_LEN - 1 - MAX_SCORE_LEN - 1
		table_text = f"<b>{title}</b>\n<pre>\n"

		for i, (uid, score, name) in enumerate(result):
			place = "🥇🥈🥉"[i] if i < 3 else str(i + 1).ljust(MAX_PLACE_LEN)
			display_name = (name[:MAX_DISPLAY_NAME_LEN - 2] + "..") if len(name) > MAX_DISPLAY_NAME_LEN else name.ljust(MAX_DISPLAY_NAME_LEN)
			score_str = f"{score:.2f}" if stat_type == 'e' else humanize_value_for_chars(score)
			display_score = score_str.ljust(MAX_SCORE_LEN)
			table_text += f"{place} {display_name} {display_score}\n"
		
		table_text += "</pre>"
		await message.answer(table_text, parse_mode='HTML')
	else: # output_type == 'p'
		image_data = await generate_pil_image(result, title)
		photo = BufferedInputFile(image_data.getvalue(), filename="top.png")
		await bot.send_photo(chat_id, photo=photo, caption=title)
		
	

def humanize_value_for_chars(num: int) -> str: # Переименовано
	"""Преобразует количество символов в человекочитаемый формат."""
	if num >= 1000000:
		return f"{num / 1000000:.2f}M"
	elif num >= 1000:
		return f"{num / 1000:.2f}K"
	else:
		return str(num)


def escape_markdown_v2_smart(text: str) -> str:
    """
    Экранирует символы MarkdownV2, не трогая уже экранированные.
    """
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    escaped = []
    i = 0
    while i < len(text):
        c = text[i]
        # если уже экранировано — не трогаем
        if c == "\\" and i + 1 < len(text) and text[i + 1] in escape_chars:
            escaped.append(c)
            escaped.append(text[i + 1])
            i += 2
            continue
        if c in escape_chars:
            escaped.append("\\" + c)
        else:
            escaped.append(c)
        i += 1
    return "".join(escaped)


def _detect_preescaped(text: str) -> bool: # Эта функция не используется
    """
    Определяет, содержит ли текст уже экранирование MarkdownV2.
    Эвристика: если доля `\` > 0.5% от длины — считаем экранированным.
    """
    if not text:
        return False
    count = text.count("\\")
    ratio = count / len(text)
    return ratio > 0.005

# Для разбивки текста на части по лимиту Telegram
# на будущие
TG_MAX_LENGTH = 4096
def _split_tg_message(text: str, max_len: int = TG_MAX_LENGTH) -> list[str]:
    """Разбивает текст по лимиту Telegram (4096 символов)."""
    if len(text) <= max_len:
        return [text]
    parts, current = [], ""
    for line in text.splitlines(keepends=True):
        if len(current) + len(line) > max_len:
            parts.append(current.strip())
            current = line
        else:
            current += line
    if current:
        parts.append(current.strip())
    return parts


def markdown_to_tg_v2(text: str) -> str:
    """
    Преобразует markdown в Telegram MarkdownV2 формат.

    Поддерживает:
      - # Заголовки
      - **жирный**, *курсив*
      - Списки (-, +, *)
      - Цитаты (>)
      - Ссылки [текст](url)
      - Блоки кода
    """
    escape = escape_markdown_v2_smart

    lines = text.splitlines()
    result = []
    in_code_block = False

    for line in lines:
        if not line.strip(): # Пропускаем пустые строки, сохраняя перенос
            result.append("")
            continue
        # Блоки кода
        if re.match(r"^\s*```", line):
            in_code_block = not in_code_block
            result.append("```" if in_code_block else "```")
            continue

        if in_code_block:
            result.append(line) # Внутри блока кода ничего не экранируем
            continue # <-- Добавлено

        # Заголовки
        if line.strip().startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            title = line[level:].strip()
            title = escape(title)
            if level == 1: # H1 -> *BOLD*
                result.append(f"*{title.upper()}*\n")
            elif level == 2: # H2 -> _italic_
                result.append(f"_{title}_\n")
            else: # H3+ -> • bullet
                result.append(f"• {title}\n")
            continue

        # Цитаты
        if line.strip().startswith(">"):
            content = escape(line.lstrip(">").strip())
            result.append(f"> {content}")
            continue

        # Списки
        if re.match(r"^\s*([-*+])\s+", line):
            item = re.sub(r"^\s*([-*+])\s+", "", line)
            result.append(f"• {escape(item)}")
            continue

        # --- Новая логика обработки строки с форматированием ---
        # Разбиваем строку по элементам форматирования (ссылки, жирный, курсив)
        # и экранируем только обычный текст между ними.
        parts = re.split(r'(\[.*?\]\(.*?\))|(\*\*.*?\*\*)|(\*.*?\*)', line)
        processed_line = []
        for part in filter(None, parts): # filter(None, parts) убирает пустые строки
            if not part: continue

            if part.startswith('[') and part.endswith(')'): # Ссылка [текст](url)
                match = re.match(r'\[(.*?)\]\((.*?)\)', part)
                if match:
                    processed_line.append(f"[{escape(match.group(1))}]({match.group(2)})")
            elif part.startswith('**') and part.endswith('**'): # Жирный **текст**
                processed_line.append(f"*{escape(part[2:-2])}*")
            elif part.startswith('*') and part.endswith('*'): # Курсив *текст*
                processed_line.append(f"_{escape(part[1:-1])}_")
            else: # Обычный текст
                processed_line.append(escape(part))

        result.append("".join(processed_line))

    full_text = "\n".join(result).strip()
    return full_text

def get_user_markdown_link(user_or_chat: types.User | types.Chat | None = None, user_id: int | None = None, full_name: str | None = None) -> str:
    """
	Создает Markdown-ссылку на пользователя (для parse_mode=MarkdownV2).
	Безопасно экранирует спецсимволы в имени.
	Может принимать либо объект User/Chat, либо user_id и full_name.
	"""
    if user_or_chat:
        user_id = user_or_chat.id
        full_name = getattr(user_or_chat, 'full_name', 'Unknown User')
    elif not (user_id and full_name):
        return "Неизвестный пользователь"

    escaped_name = escape_markdown_v2_smart(full_name)
    user_url = f"tg://user?id={user_id}"
    
    return f"[{escaped_name}]({user_url})"



############# Вход и выход из чата ##########
async def kick_msg(Kto: str, Kogo: str, chel: bool) -> str:
    # Извлекаем чистые имена из Markdown-ссылок для передачи в AI
    kto_name_match = re.search(r"\s*\[(.*?)\]", Kto)
    kogo_name_match = re.search(r"\s*\[(.*?)\]", Kogo)
    # Если ссылка не найдена, используем исходную строку (может быть просто имя)
    kto_name = kto_name_match.group(1) if kto_name_match else Kto
    kogo_name = kogo_name_match.group(1) if kogo_name_match else Kogo

    try:
        response = await gclient.aio.models.generate_content(
        model=GEMINI_MODEL,
        contents=f"""
**Задача:** Сгенерируй ОДНО короткое (до 25 слов) и язвительное сообщение о том, что пользователь '{kogo_name}' был кикнут пользователем '{kto_name}'. И преукрась смайлами.

**Входные данные:**
*   Кикнутый пользователь (Kogo): "{kogo_name}"
*   Тот, кто кикнул (Kto): "{kto_name}"
*   Контекст (is_bot): {chel} (true, если кикнул бот; false, если админ)

**Логика и Грамматика (ОЧЕНЬ ВАЖНО):**
1.  Твоя задача — создать готовое, грамматически корректное предложение.
2.  **Род:** Определи род '{kto_name}' по его имени/нику. Если это женское имя ("Мария", "Нейросеть"), используй глаголы женского рода ("решила", "указала"). Если род неясен (ники вроде "admin", "dv_pod"), используй мужской род по умолчанию.
3.  **Падежи:** Правильно склоняй имена '{kto_name}' и '{kogo_name}' в зависимости от контекста предложения.
4.  **Контекст:** Если `is_bot` это `true`, высмей '{kogo_name}' за то, что его выгнал бездушный бот '{kto_name}'. Если `false`, высмей '{kogo_name}' за то, что его изгнал могущественный админ '{kto_name}'.

**Финальное правило:**
Твой ответ должен быть строкой, готовой для отправки в Telegram с `parse_mode=MarkdownV2`.
Это значит, что все зарезервированные символы (`_`, `*`, `[`, `]`, `(`, `)`, `~`, `\``, `>`, `#`, `+`, `-`, `=`, `|`, `{{`, `}}`, `.`, `!`) в тексте, **кроме самих имен**, должны быть экранированы символом `\`.
Например, если хочешь закончить предложение точкой, используй `\\.`.

**Примеры правильной обработки:**
*   Входные: Kto: 'Мария', Kogo: 'user123' -> Ответ: "Мария элегантно указала user123 на дверь\\."
*   Входные: Kto: 'dv_pod', Kogo: 'новичок' -> Ответ: "dv_pod решил, что новичок здесь явно лишний\\!"
*   Входные: Kto: 'Нейросеть', Kogo: 'Спамер' -> Ответ: "Нейросеть сочла Спамера цифровым мусором и стерла его\\."
""",
        )
        # Получаем сгенерированный текст
        generated_text = response.text.strip()

        if not generated_text:
            raise ValueError("Empty response from AI")

        final_text = generated_text.replace(kto_name, Kto).replace(kogo_name, Kogo)
        return final_text
    except Exception as e:
        # Если ИИ не ответил или произошла ошибка, возвращаем стандартное сообщение
        logging.error(f"AI generation failed in kick_msg: {e}", exc_info=True)
        return f"👋 {Kto} изгнал\\(а\\) пользователя {Kogo}\\."


# Функция для применения прогрессивной блокировки
async def apply_progressive_ban(chat_id: int, user_id: int, reason_log: str):
    """
    Вычисляет длительность прогрессивной блокировки, применяет её к пользователю и логирует действие.
    """
    try:
        key = f"chat:{chat_id}:ban_counter:{user_id}"
        
        # Увеличиваем счетчик банов для пользователя
        ban_count = await r.incr(key)
        
        # Устанавливаем TTL на 1 год при первом нарушении, чтобы счетчик сбрасывался (366 дней для запаса)
        if ban_count == 1:
            await r.expire(key, int(timedelta(days=31).total_seconds()))

        now = datetime.now()
        
        if ban_count == 1:
            # 1-е нарушение: бан на 1 день
            ban_until = now + timedelta(days=1)
            duration_str = "1 день"
        elif ban_count == 2:
            # 2-е нарушение: бан на 1 месяц (30 дней)
            ban_until = now + timedelta(days=30)
            duration_str = "1 месяц"
        else:
            # 3-е и последующие нарушения: бан на 1 год
            ban_until = now + timedelta(days=365)
            duration_str = "1 год"
            
        ban_until_timestamp = int(ban_until.timestamp())

        await bot.ban_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            until_date=ban_until_timestamp
        )
        logging.warning(f"Пользователь {user_id} забанен на {duration_str} в чате {chat_id}. Причина: {reason_log}")
    except Exception as e:
        logging.error(f"Failed to ban user {user_id} in chat {chat_id} (in apply_progressive_ban): {e}")

# Выход из чата
@dp.chat_member(ChatMemberUpdatedFilter(LEAVE_TRANSITION))
async def off_member(event: ChatMemberUpdated):
    # --- НОВАЯ ЛОГИКА: Проверяем, не бота ли удалили ---
    if event.new_chat_member.user.id == bot.id:
        chat_id = event.chat.id
        logging.warning(f"Бот был удален из чата '{event.chat.title}' ({chat_id}). Начинаю полную очистку данных.")
        
        # Используем SCAN для поиска всех ключей, связанных с этим чатом
        keys_to_delete = [key async for key in r.scan_iter(f"chat:{chat_id}:*")]
        
        if keys_to_delete:
            await r.delete(*keys_to_delete)
            logging.info(f"Удалено {len(keys_to_delete)} ключей из Redis для чата {chat_id}.")
        else:
            logging.info(f"Не найдено ключей для удаления для чата {chat_id}.")
        return # Завершаем обработку, т.к. бот больше не в чате

    member = event.new_chat_member.user
    chat_id = event.chat.id

    # --- НОВАЯ ЛОГИКА: Проверка на быстрый выход (во время отсчета) ---
    countdown_key = f"chat:{chat_id}:in_countdown"
    if await r.sismember(countdown_key, member.id):
        # Пользователь вышел во время отсчета. Баним его.
        logging.warning(f"Пользователь {member.full_name} ({member.id}) покинул чат во время отсчета. Применяется бан.")
        reason = f"покинул чат во время приветственного отсчета"
        await apply_progressive_ban(chat_id, member.id, reason)
        # Удаляем его из сета, чтобы new_member не пытался с ним работать
        await r.srem(countdown_key, member.id)
        return # Завершаем обработку, не нужно отправлять прощальное сообщение

    # --- СТАРАЯ ЛОГИКА: Обработка выхода обычного пользователя ---
    logging.info(f"Выход: из чата {event.chat.title} - {member.full_name}")

    # 1. Централизованно удаляем все связанные с проверкой сообщения и данные из Redis.
    # Это решает проблему с неполной очисткой.
    await cleanup_verification_data(event.chat.id, member.id)

    # 2. Определяем причину выхода и отправляем соответствующее сообщение.
    if isinstance(event.new_chat_member, ChatMemberBanned):
        # Если бан инициирован ботом (например, за провал проверки),
        # то сообщение уже отправлено из соответствующей функции (check_new_members).
        # Пропускаем, чтобы избежать дублирования.
        if event.from_user.is_bot:
            logging.info(f"Бан для {member.full_name} инициирован ботом. Сообщение в off_member пропускается.")
            return

        # Создаем надежные ссылки на пользователей, используя .url вместо .username
        kicked_user_link = get_user_markdown_link(member)
        admin_user_link = get_user_markdown_link(event.from_user)
        
        # Генерируем и отправляем сообщение о бане (для ручных банов админами)
        kick_message_text = await kick_msg(admin_user_link, kicked_user_link, event.from_user.is_bot)
        try:
            await event.answer(kick_message_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
        except TelegramBadRequest as e:
            if "can't parse entities" in str(e).lower():
                logging.warning(f"Failed to send kick message due to markdown error. Falling back to simple message. Error: {e}")
                fallback_text = f"👋 {admin_user_link} изгнал(а) пользователя {kicked_user_link}\."
                await event.answer(fallback_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
            else:
                raise

    elif isinstance(event.new_chat_member, ChatMemberLeft):
		# Обычный выход пользователя
        await event.answer(f"👋 Гудбай {get_user_markdown_link(member)}", parse_mode="MarkdownV2", disable_web_page_preview=True)

# Обработчик новых участников
@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def new_member(event: ChatMemberUpdated):
    new_member = event.new_chat_member.user    
    chat_id = event.chat.id
    chat_Ti = event.chat.title
    user_id = new_member.id    
    logging.info(f"Вход! Новенького в чате {chat_Ti} - {new_member.full_name} ({user_id})")

    if not new_member.is_bot:
        # --- НОВАЯ ЛОГИКА: Регистрация пользователя в списке ожидания ---
        countdown_key = f"chat:{chat_id}:in_countdown"
        await r.sadd(countdown_key, user_id) # Добавляем пользователя в сет отсчета
        await r.expire(countdown_key, 60) # 60 секунд - с запасом

        await user_lock_unlock(user_id, chat_id, st="lock")
        # --- Список весёлых сообщений для обратного отсчёта ---
        countdown_templates = [
            '<b><a href="{url}">{name}</a></b>, держись! До полного погружения в наш чат осталось {sec} секунд. 🚀',
            'До высадки на нашей планете <b><a href="{url}">{name}</a></b> осталось {sec}... секунд! Приготовиться! 👽',
            'Тик-так, тик-так... ⏰ <b><a href="{url}">{name}</a></b>, у тебя есть {sec} секунд, чтобы придумать первую шутку. 😉',
            'Внимание! Запуск <b><a href="{url}">{name}</a></b> в чат через {sec} секунд. Всем пристегнуть ремни! 💥',
            'Ура! 🥳 Наш новый участник <b><a href="{url}">{name}</a></b> приземлится через {sec} секунд. Готовим конфетти! 🎉',
            'Осталось {sec} секунд до того, как <b><a href="{url}">{name}</a></b> официально станет частью нашей команды! 🥳 Готовьте мемы! 😂',
            'Начинаем финальный отсчёт для <b><a href="{url}">{name}</a></b>! {sec}... секунд и... бум! 🎉 Ты с нами!',
            'Термоядерный двигатель запущен! 💥 <b><a href="{url}">{name}</a></b>, до твоей высадки осталось всего {sec} секунд. Не забудь кислородный баллон! 👨‍🚀',
            'Пока {sec} секунд не истекли, <b><a href="{url}">{name}</a></b>, у тебя есть время решить, кто твой любимый персонаж из "Звёздных войн". 🤔 Готовься к дебатам! ⚔️',
            'Наш чат-бот готовится поприветствовать <b><a href="{url}">{name}</a></b>! До этого торжественного момента осталось {sec} секунд. ⏳'
        ]
        # Выбираем одну случайную фразу для этого пользователя
        chosen_template = random.choice(countdown_templates)
        # Используем html.escape для имени, чтобы избежать проблем с разметкой
        full_name_html = html.escape(new_member.full_name)

        # Динамический отсчёт
        initial_message = chosen_template.format(url=new_member.url, name=full_name_html, sec=10)
        countdown_msg = await bot.send_message(
            chat_id,
            initial_message,
            parse_mode="HTML"
        )
        for sec in range(9, -1, -1):
            try:
                await asyncio.sleep(1)
                # Если пользователь на месте, редактируем сообщение
                next_message = chosen_template.format(url=new_member.url, name=full_name_html, sec=sec)
                await countdown_msg.edit_text(next_message, parse_mode="HTML")
            except TelegramBadRequest as e:
                if "message to edit not found" in e.message.lower():
                    logging.warning(f"Сообщение для отсчета {countdown_msg.message_id} было удалено. Прерываю отсчет.")
                    await r.srem(countdown_key, user_id) # Очищаем ключ
                    return # Сообщения нет, делать нечего
            except Exception as e:
                logging.error(f"Ошибка при обновлении отсчёта или проверке статуса: {e}")
                await r.srem(countdown_key, user_id) # Очищаем ключ
                await countdown_msg.delete()
                return
        # --- ФИНАЛЬНАЯ ПРОВЕРКА ---
        # Проверяем, не покинул ли пользователь чат во время отсчета (off_member должен был удалить его из сета).
        if not await r.sismember(countdown_key, user_id):
            logging.info(f"Пользователь {user_id} покинул чат во время отсчета. Обработка завершена в off_member.")
            await countdown_msg.delete()
            return

        # Если пользователь дождался, удаляем его из временного сета и продолжаем
        await r.srem(countdown_key, user_id)
        try:
            await countdown_msg.delete()
        except Exception as e:
            logging.warning(f"Не удалось удалить сообщение отсчета: {e}")

        # Сообщение с просьбой картинки (HTML + экранирование)
        full_name_html = (
            f'<a href="{new_member.url}">{html.escape(new_member.full_name)}</a>'
        )
        check_msg = await bot.send_message(
            chat_id,
            f"👋 Привет, {full_name_html}!\n"
            f"Пройди простую проверку.\n\n"
            f"<b>Ответь</b> на <b>ЭТО</b> сообщение <u>картинкой велосипеда</u>.\n"
            f"Иначе не сможешь отправлять сообщения 🤐 "
            f"и вскоре тебя исключат 👢💥🍑. (⌛{TIME_TO_BAN_HOURS}ч.)",
            parse_mode="HTML"
        )
        # Сохраняем все id сообщений проверки
        check_data = {
            "message_id": check_msg.message_id,
            "full_name": new_member.full_name,
            "join_time": int(time.time()),
            "notified": False,
            "reminder_id": None,
            "ban_id": None
        }
        await r.hset(f"chat:{chat_id}:new_user_join", new_member.id, json.dumps(check_data))
        # Вся логика напоминания и бана теперь только в check_new_members

    else:
        # --- НОВАЯ ЛОГИКА: Улучшенное и безопасное сообщение о боте ---
        await user_lock_unlock(user_id, chat_id, st="lock")
        # Используем централизованную функцию для создания безопасной ссылки
        bot_link = get_user_markdown_link(new_member)
        text = (
            f"🚨 Внимание, в чат проник бот {bot_link}\\!\n"
            f"Админы, примите решение: `Принят` или `Бан` в ответ на это сообщение\\."
        )
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="MarkdownV2"
        )
        
async def generate_image_description(image: Image.Image) -> bool | None:
    """
    Определяет, есть ли на картинке велосипед.
    Возвращает True, если есть, False, если нет, и None в случае ошибки или нечеткого ответа.
    """
    try:
        # Формируем запрос (можно кастомизировать)
        prompt = "На этой картинке есть велосипед? Ответь одним словом: True или False."

        # Генерируем контент асинхронно
        response = await gclient.aio.models.generate_content(
            model=GEMINI_MODEL,
            contents=[image, prompt]
        )
        response_text = response.text.strip().lower()
        return response_text == 'true' if response_text in ['true', 'false'] else None
    except Exception as e:
        logging.error(f"Error in generate_image_description: {e}", exc_info=True)
        return None

## Ограничиваем возможности пользователя (блокируем отправку сообщений) | расширеный
async def user_lock_unlock(user_id: int, chat_id: int, **kwargs):
    try:
        if kwargs['st'] == 'lock':  # Блокировка
            await bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user_id,
                permissions=ChatPermissions(
                    can_send_photos=True, # Разрешаем только фото для проверки
                ),
                until_date=0  # 0 или None – ограничение бессрочное
            )
            logging.info(f"{user_id} - Отправка сообщений заблокирована.")

        elif kwargs['st'] == "unlock":  # Разблокировка
            # Снимаем все ограничения, возвращая стандартные права
            await bot.restrict_chat_member(
                chat_id=chat_id,
                user_id=user_id,
                until_date=datetime.now() + timedelta(seconds=45),  # ограничение на 45 секунд
                permissions=ChatPermissions(
                    can_send_messages=True,					
                )
            )
            logging.info(f"{user_id} - Отправка сообщений разблокирована.")

    except Exception as e:
        logging.error(f"Error in user_lock_unlock: {e}", exc_info=True)
        await bot.send_message(chat_id, f"Ошибка: {str(e)}")

# Функция проверки и исключения
async def cleanup_verification_data(chat_id: int, user_id: int):
    """
    Централизованно удаляет все сообщения, связанные с проверкой, и запись из Redis.
    """
    key = f"chat:{chat_id}:new_user_join"
    try:
        data_str = await r.hget(key, user_id)
        if not data_str:
            logging.info(f"Нет данных для очистки для пользователя {user_id} в чате {chat_id}.")
            return

        # Сначала удаляем запись из Redis, чтобы предотвратить повторные попытки
        await r.hdel(key, user_id)
        logging.info(f"Запись для пользователя {user_id} удалена из Redis.")

        data = json.loads(data_str)

        # Собираем все ID сообщений в один список
        message_ids_to_delete = [
            data.get("message_id"),
            data.get("reminder_id"),
            data.get("ban_id")
        ]

        for msg_id in message_ids_to_delete:
            if msg_id:
                try:
                    await bot.delete_message(chat_id, msg_id)
                except Exception as e:
                    if 'message to delete not found' not in str(e).lower():
                        logging.warning(f"Ошибка при удалении сообщения {msg_id} в чате {chat_id}: {e}")
    except Exception as e:
        logging.error(f"Критическая ошибка в cleanup_verification_data для user {user_id} в чате {chat_id}: {e}", exc_info=True)

async def check_new_members():
    async for key in r.scan_iter("chat:*:new_user_join"):
        chat_id = key.split(":")[1]
        members = await r.hgetall(key)
        for user_id, data in members.items():
            try:
                data = json.loads(data)
                join_time = data["join_time"]
                current_time = int(time.time())
                time_elapsed = current_time - join_time

                if time_elapsed >= NOTIFY_AFTER_SECONDS and not data.get('notified', False):
                    user_nm = data.get('full_name', f'user_{user_id}')
                    msg_id = data.get('message_id')
                    # --- УЛУЧШЕННАЯ И БЕЗОПАСНАЯ ЛОГИКА СОЗДАНИЯ ССЫЛКИ ---
                    # 1. Берем ID чата по модулю, чтобы он всегда был положительным.
                    abs_chat_id_str = str(abs(int(chat_id)))
                    # 2. Если это супергруппа (начинается с '100'), отрезаем этот префикс.
                    #    В противном случае используем ID как есть.
                    chat_id_str = abs_chat_id_str[3:] if abs_chat_id_str.startswith('100') else abs_chat_id_str
                    user_link = get_user_markdown_link(user_id=int(user_id), full_name=user_nm)
                    reminder = await bot.send_message(
                        chat_id=int(chat_id),
                        text=f"⏰ {user_link}, не забудь пройти проверку\\!\nОсталось {int((TIME_TO_BAN_SECONDS - time_elapsed)//60)} мин\\. до 👢💥🍑\\.\nОтветь на [запрос бота](https://t.me/c/{chat_id_str}/{msg_id})",
                        parse_mode="MarkdownV2"
                    )
                    data['notified'] = True
                    data['reminder_id'] = reminder.message_id
                    await r.hset(key, user_id, json.dumps(data))

                if time_elapsed >= TIME_TO_BAN_SECONDS: # Изменено на >= для точности
                    user_nm = data.get('full_name', f'user_{user_id}')
                    # Применяем прогрессивный бан
                    reason = f"провал проверки ({user_nm})"
                    await apply_progressive_ban(int(chat_id), int(user_id), reason)

                    # --- ЯВНАЯ ОТПРАВКА СООБЩЕНИЯ О БАНЕ ---
                    bot_user = await bot.get_me()
                    bot_link = get_user_markdown_link(bot_user)
                    # Создаем "утиный" объект пользователя (duck-typing) для передачи в get_user_markdown_link
                    kicked_user_link = get_user_markdown_link(user_id=int(user_id), full_name=user_nm)
                    kick_message_text = await kick_msg(bot_link, kicked_user_link, True)

                    try:
                        ban_msg = await bot.send_message(
                            chat_id=int(chat_id),
                            text=kick_message_text,
                            parse_mode="MarkdownV2",
                            disable_web_page_preview=True
                        )
                    except TelegramBadRequest as e:
                        if "can't parse entities" in str(e).lower():
                            logging.warning(f"Failed to send kick message due to markdown error. Falling back to simple message. Error: {e}")
                            fallback_text = f"👋 {bot_link} изгнал(а) пользователя {kicked_user_link}\."
                            ban_msg = await bot.send_message(
                                chat_id=int(chat_id),
                                text=fallback_text,
                                parse_mode="MarkdownV2",
                                disable_web_page_preview=True
                            )
                        else:
                            raise

                    # Запускаем отложенное удаление сообщения о бане
                    asyncio.create_task(del_msg_delay(ban_msg, CLEANUP_AFTER_SECONDS))

                    # Очищаем данные и сообщения, связанные с проверкой
                    await cleanup_verification_data(int(chat_id), int(user_id))
            except TelegramBadRequest as e:
                if 'chat not found' in e.message.lower():
                    logging.warning(f"Chat {chat_id} not found. Cleaning up all stale Redis data for this chat.")
                    keys_to_delete = [key async for key in r.scan_iter(f"chat:{chat_id}:*")]
                    if keys_to_delete:
                        await r.delete(*keys_to_delete)
                        logging.info(f"Удалено {len(keys_to_delete)} ключей из Redis для чата {chat_id}.")
                    break # Прерываем цикл по пользователям этого чата и переходим к следующему ключу чата
                else:
                    logging.error(f"Telegram bad request error for user {user_id} in chat {chat_id}: {e}", exc_info=True)
            except Exception as e:
                logging.error(f"Error processing new member {user_id} in chat {chat_id} (in check_new_members): {e}", exc_info=True)

# приветствие нового участника
@dp.message(Command("hello_m"))
async def cmd_info(message: Message):
    if message.from_user.id in await get_admins(message.chat.id):
        if len(message.text) < 9:
            await message.reply("Напиши текст приветствия после команды /hello_m")
            return
        # Сохраняем сообщение в Redis
        await r.set(f"chat:{message.chat.id}:Hello_msg",message.html_text.split(' ', 1)[1])
        await message.reply(f"🫡")


async def _handle_verification_message(message: Message) -> bool:
    """
    Обрабатывает все сообщения, связанные с верификацией новых пользователей.
    Возвращает True, если сообщение было обработано, и False в противном случае.
    """
    # Вспомогательная функция для надежного извлечения ID пользователя из ссылки в сообщении.
    # Она будет использоваться и для проверки ответа пользователя, и для команд админа.
    def extract_user_id_from_message(msg: Message) -> int | None:
        # 1. Try to get from entities (most reliable for user-sent messages)
        if msg.entities:
            for entity in msg.entities:
                # Ищем ссылку вида <a href="tg://user?id=12345">...</a>
                if entity.type == "text_link" and entity.url and "tg://user?id=" in entity.url:
                    try:
                        return int(entity.url.split("=")[-1])
                    except (ValueError, IndexError):
                        continue

        # 2. Fallback to parsing html_text (for bot-sent messages)
        if msg.html_text:
            match = re.search(r'href="tg://user\?id=(\d+)"', msg.html_text)
            if match:
                try:
                    return int(match.group(1))
                except (ValueError, IndexError):
                    pass # Should not happen with this regex, but good to be safe

        return None # Если не нашли ни одним из способов

    chat = message.chat
    chat_id = chat.id
    user_id = message.from_user.id
    key_u_j = f"chat:{chat_id}:new_user_join"

    # --- БЛОК 1: Обработка сообщений от пользователей, проходящих верификацию ---
    is_user_under_verification = await r.hexists(key_u_j, user_id)

    if is_user_under_verification:
        # ПРАВИЛО 1: Пересланное фото от нового юзера -> немедленный бан.
        if message.photo and (message.forward_from or message.forward_from_chat or message.forward_from_message_id):
            try:
                await message.delete()
            except TelegramBadRequest as e:
                if "message to delete not found" not in str(e).lower():
                    raise
            await apply_progressive_ban(chat_id, user_id, "пересланное фото от нового пользователя")
            await cleanup_verification_data(chat_id, user_id)
            # await message.answer('Вы были забанены за пересланное фото. Новым пользователям запрещено отправлять пересланные фото.')
            return True

        # ПРАВИЛО 2: Пользователь ответил на сообщение-проверку.
        if message.reply_to_message and message.reply_to_message.from_user.is_bot:
            verified_user_id_from_reply = extract_user_id_from_message(message.reply_to_message)
            # Убедимся, что он отвечает на СВОЮ проверку
            if verified_user_id_from_reply and verified_user_id_from_reply == user_id:
                if message.photo:
                    # Это правильный сценарий, обрабатываем фото.
                    try:
                        file = await bot.get_file(message.photo[1].file_id)
                        image_bytes = (await bot.download_file(file.file_path)).read()
                        image = Image.open(BytesIO(image_bytes))
                        description = await generate_image_description(image)

                        if description is True:
                            member_status = await bot.get_chat_member(chat_id, user_id)
                            if member_status.status not in ("left", "kicked", "banned"):
                                await user_lock_unlock(user_id, chat_id, st="unlock")
                                user_obj = await bot.get_chat(chat_id=user_id)
                                FNAME = get_user_markdown_link(user_obj) # get_user_markdown_link уже экранирует
                                hell_msg = (await r.get(f"chat:{chat_id}:Hello_msg") or f"Поприветствуйте {FNAME}, нового участника! 👋\n").replace('FNAME', FNAME)
                                await message.answer(hell_msg, parse_mode="MarkdownV2", disable_web_page_preview=True)
                                await cleanup_verification_data(chat_id, user_id)
                            else:
                                logging.warning(f"Пользователь {user_id} покинул чат до завершения проверки.")
                                await cleanup_verification_data(chat_id, user_id)
                        elif description is False:
                            # AI сказал "False" - велосипеда нет
                            await del_msg_delay(await message.reply("На этой картинке нет велосипеда. Попробуй другую. 🚲"))
                            try:
                                await message.delete()
                            except TelegramBadRequest as e:
                                if "message to delete not found" not in str(e).lower(): raise
                        else:
                            # description is None - ошибка или неожиданный ответ
                            await del_msg_delay(await message.reply("Не удалось распознать изображение. Пожалуйста, попробуйте отправить другую, более четкую картинку."))
                            try:
                                await message.delete()
                            except TelegramBadRequest as e:
                                if "message to delete not found" not in str(e).lower(): raise
                    except Exception as e:
                        logging.error(f"Error processing verification photo in _handle_verification_message: {e}", exc_info=True)
                        await del_msg_delay(await message.reply(f"Не удалось обработать фото. Попробуйте еще раз."))
                return True

        # ПРАВИЛО 3: Любое другое сообщение от пользователя на верификации -> удаление и напоминание.
        if message.photo:
            try:
                await message.delete()
            except TelegramBadRequest as e:
                if "message to delete not found" not in str(e).lower(): raise # Удаляем фото
            await del_msg_delay(await message.answer('Пожалуйста, отправьте фото в ответ на сообщение бота.')) # Отправляем и удаляем напоминание
        return True

    # --- БЛОК 2: Обработка ответов от админов на сообщения о верификации ---
    if message.reply_to_message and message.reply_to_message.from_user.is_bot:
        verified_user_id = extract_user_id_from_message(message.reply_to_message)
        if not verified_user_id:
            # Улучшаем сообщение об ошибке, чтобы админ понял, в чем дело.
            # Скорее всего, он ответил не на то сообщение бота (например, на уведомление о баяне).
            await del_msg_delay(await message.reply("Не удалось определить пользователя. Убедитесь, что вы отвечаете на сообщение о входе нового участника или на запрос о проверке."))
            return True

        # Проверяем, что проверка ещё активна
        if not await r.hexists(key_u_j, verified_user_id):
            await del_msg_delay(await message.reply("Уже всё, поздно 😏"))
            return True # Сообщение обработано

        admins = await get_admins(chat_id)
        if user_id not in admins or not message.text:
            await del_msg_delay(await message.reply("Только админ может принять решение."))
            return True

        text_lower = message.text.lower()
        if "принят" in text_lower:
            await user_lock_unlock(verified_user_id, chat_id, st="unlock")
            user_obj = await bot.get_chat(chat_id=verified_user_id)
            FNAME = get_user_markdown_link(user_obj) # get_user_markdown_link уже экранирует
            hell_msg = (await r.get(f"chat:{chat_id}:Hello_msg") or f"Поприветствуйте {FNAME}, нового участника! 👋\n").replace('FNAME', FNAME)
            await message.answer(hell_msg, parse_mode="MarkdownV2", disable_web_page_preview=True)
            await cleanup_verification_data(chat_id, verified_user_id)
            
        elif "бан" in text_lower:
            reason = f"команда 'бан' от администратора {message.from_user.full_name}"
            await apply_progressive_ban(chat_id, verified_user_id, reason)
            try:
                banned_user_obj = await bot.get_chat(verified_user_id)
                banned_user_link = get_user_markdown_link(banned_user_obj)
                admin_user_link = get_user_markdown_link(message.from_user)
                kick_message_text = await kick_msg(admin_user_link, banned_user_link, False)
                
                try:
                    ban_msg = await bot.send_message(chat_id, kick_message_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
                except TelegramBadRequest as e:
                    if "can't parse entities" in str(e).lower():
                        logging.warning(f"Failed to send kick message due to markdown error. Falling back to simple message. Error: {e}")
                        fallback_text = f"👋 {admin_user_link} изгнал(а) пользователя {banned_user_link}\."
                        ban_msg = await bot.send_message(chat_id, fallback_text, parse_mode="MarkdownV2", disable_web_page_preview=True)
                    else:
                        raise # Перебрасываем другие ошибки
                
                asyncio.create_task(del_msg_delay(ban_msg, CLEANUP_AFTER_SECONDS))
                
            except Exception as e:
                logging.error(f"Error preparing or sending ban message in _handle_verification_message: {e}", exc_info=True)
            try:
                await message.delete()
            except TelegramBadRequest as e:
                if "message to delete not found" not in str(e).lower():
                    raise
            await cleanup_verification_data(chat_id, verified_user_id)
        else:
            await del_msg_delay(await message.reply("Можно только 'Принят!' или 'Бан!'"))
        return True

    return False # Это обычное сообщение, не связанное с верификацией

# Слушать сообщения чата
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}), ~F.text.startswith('/')) # Игнорируем команды
async def save_group_message(message: Message):
    # --- 1. Обработка верификации и спама от новых пользователей ---
    if await _handle_verification_message(message):
        return # Если сообщение было частью верификации, прекращаем обработку

    # --- 2. Обработка обычных сообщений ---
    logging.debug(f"Processing regular message in chat {message.chat.id}")
    user = message.from_user
    chat = message.chat
    chat_nm = chat.title
    user_name = user.username or "None"
    full_name = user.full_name or "БезыНя-шка"
    message_data = {}
    mtext = ''

    # База баянов
    bayan = False
    if message.photo or message.video:
        bayan = await check_bayan(message)

    # --- 3. Сборка данных для сохранения ---
    message_data['id'] = message.message_id
    if message.reply_to_message:  # Добавлена проверка на None
        message_data['reply_to'] = message.reply_to_message.message_id
    message_data['user_name'] = user_name
    message_data['full_name'] = full_name

    if message.forward_from_chat:
        mtext = f"Переслано от {message.forward_from_chat.title}\n"	

    if message.text:
        message_data['text'] = mtext + message.text

    elif message.caption:
        if message.photo:
            mtext = mtext + "[foto]\n"
        elif message.animation:
            mtext = mtext + "[gif]\n"
        elif message.video:
            mtext = mtext + "[video]\n"
        elif message.audio:
            mtext = mtext + "[audio]\n"
                
        message_data['text'] = mtext + message.caption	
    else:
        return
    
    # --- 4. Обновление статистики и сохранение в Redis ---
    # Получаем текущий месяц в формате YYYY-MM
    current_period = datetime.now().strftime("%Y-%m")
    
    # Ключи для статистики
    # Общие
    key_msg_all = f"chat:{chat.id}:count_u_msg:all_time"
    key_len_all = f"chat:{chat.id}:count_u_len:all_time"
    key_byn_all = f"chat:{chat.id}:count_u_byn:all_time"
    # За текущий месяц
    key_msg_month = f"chat:{chat.id}:count_u_msg:{current_period}"
    key_len_month = f"chat:{chat.id}:count_u_len:{current_period}"
    key_byn_month = f"chat:{chat.id}:count_u_byn:{current_period}"

    async with r.pipeline() as pipe:
        # Обновляем общую статистику
        pipe.zincrby(key_msg_all, 1, user.id)
        pipe.zincrby(key_len_all, len(message_data['text']), user.id)
        if bayan:
            pipe.zincrby(key_byn_all, 1, user.id)

        # Обновляем статистику за текущий месяц
        pipe.zincrby(key_msg_month, 1, user.id)
        pipe.zincrby(key_len_month, len(message_data['text']), user.id)
        if bayan:
            pipe.zincrby(key_byn_month, 1, user.id)
        
        # Сохранение сообщения в историю
        pipe.lpush(f"chat:{chat.id}:history", json.dumps(message_data))
        pipe.ltrim(f"chat:{chat.id}:history", 0, MAX_HISTORY - 1)
        
        await pipe.execute()

    logging.debug(f"Message {message.message_id} saved and stats updated for period {current_period} in {chat_nm}")

@dp.edited_message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}), ~F.text.startswith('/'))
async def handle_edited_message(message: Message):
    """Обрабатывает отредактированные сообщения и обновляет их в истории Redis."""
    chat_id, message_id = message.chat.id, message.message_id
    key = f"chat:{chat_id}:history"
    logging.info(f"Attempting to update edited message {message_id} in chat {chat_id}")

    history_json = await r.lrange(key, 0, EDIT_SEARCH_DEPTH - 1)
    for i, msg_json in enumerate(history_json):
        try:
            msg_data = json.loads(msg_json)
            if msg_data.get('id') == message_id:
                new_text = message.text or message.caption
                if not new_text: return

                msg_data['text'] = new_text
                
                await r.lset(key, i, json.dumps(msg_data))
                logging.info(f"Message {message_id} updated in history at index {i}.")
                return
        except (json.JSONDecodeError, TypeError): continue
    logging.warning(f"Edited message {message_id} not found in history for update.")

async def set_main_menu(bot: Bot):
    """
    Создает и устанавливает основное меню команд для бота.
    """
    # Создаем список команд с их описаниями
    main_menu_commands = [
        # BotCommand(command="/start", description="👋 Начало работы | Приветствие"),
        BotCommand(command="sum", description="📝 Сделать сводку последних сообщений"),
        BotCommand(command="top_u", description="🏆 Показать топ активных пользователей"),
        # BotCommand(command="анекдот", description="😂 Рассказать анекдот"),
        # BotCommand(command="/right", description="🔒 Проверить права (бота или юзера)"),
        BotCommand(command="del", description="🗑️ (Админ) Удалить сообщение"),
        BotCommand(command="run_info", description="ℹ️ Время запуска бота"),
        BotCommand(command="hello_m", description="✍️ (Админ) Установить приветствие")
    ]

    # Устанавливаем команды для бота
    await bot.set_my_commands(main_menu_commands)


# Настройка планировщика
def setup_scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(check_new_members, 'interval', minutes=1) 
    scheduler.start()


# Запуск бота
async def main():
    print("✅ Бот запущен!")
    setup_scheduler() # Настройка планировщика
    await init_redis() # Инициализация Redis
    await set_main_menu(bot) # Устанавливаем меню команд
    await dp.start_polling(bot) # Обработка сообщений TG

if __name__ == "__main__":
    asyncio.run(main())
