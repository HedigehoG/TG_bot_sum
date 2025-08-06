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

from annotated_types import LowerCase
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import redis.asyncio as redis
from dotenv import load_dotenv
from os import getenv
import random

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command, ChatMemberUpdatedFilter, JOIN_TRANSITION, LEAVE_TRANSITION, CommandObject
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import ChatMemberAdministrator, ChatMemberOwner, ChatMemberRestricted, ChatMemberLeft, \
	ChatMemberBanned, ChatMemberMember, ChatPermissions, ChatMemberUpdated, InlineKeyboardMarkup, InlineKeyboardButton, \
	Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile, BotCommand
from aiogram.exceptions import TelegramForbiddenError

import html  # Для экранирования HTML-символов в именах пользователей

from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import imagehash

from google import genai
from google.genai import types as gtypes


# Настройка базовой конфигурации логирования
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', 
					level=logging.INFO)

# Загрузка конфигурации
load_dotenv()
ADMIN_ID = getenv("ADMIN_ID")
TELEGRAM_TOKEN = getenv("TELEGRAM_TOKEN")

# Настройка Gemini
GOOGLE_API_KEY = getenv("GOOGLE_API_KEY")
gclient = genai.Client(api_key=GOOGLE_API_KEY)

#RAPIDAPI_KEY = getenv("RAPIDAPI_KEY")
REDIS_HOST = getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(getenv("REDIS_PORT", 6379))


ADMIN_IDS = {ADMIN_ID}

# Конфигурация лимитов
LOCK_FOR_SUMMARIZE = set()

MAX_HISTORY = 10000  # Всего сообщений истории
MAX_TO_GPT = 2000  # Сообщений в гпт
MIN_TO_GPT = 150
MAX_SUM = 3500  # сивлолов для ответа суммар
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
		# Проверка соединения
		await r.ping()
		logging.info("Connected to Redis successfully")
	except Exception as e:
		logging.error(f"Failed to connect to Redis: {e}")
		# Здесь можно предусмотреть повторные попытки подключения или другие действия

async def get_admins(chat_id: int):
	# Подгрузка списка админов
	admins = await bot.get_chat_administrators(chat_id)
	admin_ids = {admin.user.id for admin in admins}
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
	await smg_obj.delete()

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
	req = 'on' # Для тестов, чтобы не проверять в бд

	if req == 'on':
		# 1. Проверяем, занят ли чат
		if chat_id in LOCK_FOR_SUMMARIZE:
			await message.reply("ХарЭ столько тыкать. Пожалуйста, подождите.")
			return # Прекращаем выполнение, если чат занят

		# 2. Добавляем чат в список обрабатываемых (захватываем блокировку)
		LOCK_FOR_SUMMARIZE.add(chat_id)

		# --- НОВАЯ, УЛУЧШЕННАЯ ЛОГИКА ПАРСИНГА ---
		args = command.args.split() if command.args else []
		
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
			# 4. Снимаем блокировку, независимо от результата выполнения try/except
			if chat_id in LOCK_FOR_SUMMARIZE: # Проверка на всякий случай
				LOCK_FOR_SUMMARIZE.remove(chat_id)
				logging.info(f"Снятие блокировки для чата {chat_id}. Активные блокировки: {LOCK_FOR_SUMMARIZE}")

	elif req == 'off':
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
	
	new_messages = []
	msg_old_id = None	

	ttl = await check_daily_limit(chat_id, user.username)
	if (ttl > 0):
		await del_msg_delay(await message.answer(f"❌ Достигнут лимит запросов суммаризации!\n Подождите {format_seconds(ttl)}"))
		return
	
	try:
		key = f"chat:{chat_id}:history"
		logging.info(f"Проверка получения последнего сообщения из: {key}")
		messages = await r.lrange(key, 0, 0)
		msg_last = json.loads(messages[0])
		msg_last_id = msg_last['id']
	except Exception as e:
		logging.error(f"Redis history error: {e}")
		await message.answer("Ошибка получения истории чата")
		return
	
	chat_id_str = str(chat_id)[4:]  ## обрезка индекса для ссылки на чат
	chat = message.chat
	if chat.username:
		turl = f"t.me/{chat.username}/"
	else:
		turl = f"t.me/c/{chat_id_str}/"

	if count != 0:
		logging.info(f'свой свод c-{count} s-{start}')
		messages = await r.lrange(key, start, start + count - 1)
		messages = [json.loads(message_json) for message_json in messages]
		messages.reverse()

	else:
		msg_old_id = await r.hget(f"chat:{chat_id}:last_sum", 'id')
		last_sum_id = await r.hget(f"chat:{chat_id}:last_sum", 'msg_id')

		# Преобразуем значения в целые числа, обрабатывая None
		msg_old_id = int(msg_old_id or 0)  # Если None, используем 0
		msg_last_id = int(msg_last_id or 0) # Если None, используем 0

		count = msg_last_id - msg_old_id

		#logging.info(f"chat:{chat_id}:last_sum",count , msg_last_id, msg_old_id)
		if count < MIN_TO_GPT:
			await del_msg_delay(await message.answer(f"Новых сообщений не более {count}, прочитайте сами."))
			return

		messages = await r.lrange(key, 0, count - 1)

		for message_json in messages:
			msg = json.loads(message_json)            
			if msg['id'] > msg_old_id:
				new_messages = [msg] + new_messages
			else:
				break

		messages = new_messages
		count = len(new_messages)

	if not messages:
		await del_msg_delay(await message.answer("Нет сообщений для суммаризации."))
		return

	if new_messages and (last_sum_id != 0): 
		surl = f'Предыдущий свод [тут]({turl}{last_sum_id})'
	else:
		surl = ''

	typing_task = None # Инициализируем переменную для задачи
	try:
		# 1. Создаем и запускаем фоновую задачу, которая будет слать "typing"
		async def send_typing_periodically():
			"""Отправляет 'typing' каждые 4 секунды, пока не будет отменена."""
			while True:
				await bot.send_chat_action(target_chat_id, action="typing")
				await asyncio.sleep(4) # Пауза 4 секунды (безопасно меньше лимита Telegram)

		typing_task = asyncio.create_task(send_typing_periodically())

		logging.info(f"передача 1msg-{messages[0]}\n"
					f"число-{count}, last_sum_id-{surl}, msg_old_id-{msg_old_id}")
		summary = await get_gpt4_summary(messages, turl)
		logging.info(f"Ответ gpt4: получен. Длина {len(summary)}")
		await r.lpush('gpt_answ_t', json.dumps(summary))

		if typing_task:
			typing_task.cancel()

		# <--- 4. Используем bot.send_message для отправки в target_chat_id
		sum_text = f"📝 #Суммаризация последних {count} сообщений:\n{summary}"
		if surl and not privat: # Добавляем ссылку на пред. свод только если постим в чат
			sum_text += f"\n{surl}"

		sum = await bot.send_message(
			chat_id=target_chat_id,
			text=sum_text,
			disable_web_page_preview=True,
			parse_mode=ParseMode.MARKDOWN
		)

	# <--- 5. Обработка ошибки, если бот не может написать в личку
	except TelegramForbiddenError:
		logging.error(f"Не удалось отправить сообщение пользователю {user.id}. Бот заблокирован или чат не начат.")
		await bot.send_message(target_chat_id, "Не всё так просто, спроси сам знаешь кого ))")
		return
	except Exception as e:
		logging.error(f"AI or sending error: {e}")
		await bot.send_message(target_chat_id, "Ошибка получения данных от AI или отправки сообщения.")
		return
	finally:
		if typing_task:
			typing_task.cancel()

	await upd_daily_limit(chat_id, user.username, privat)
	
	if new_messages and not privat:
		key = f"chat:{chat_id}:last_sum"
		async with r.pipeline() as pipe:
			pipe.hset(key, 'id', msg_last_id)
			pipe.hset(key, 'msg_id', sum.message_id)
			pipe.hset(key, 'text', json.dumps(summary))
			pipe.lpush(key+":all",json.dumps([msg_last_id,sum.message_id,messages,summary]))
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

	async with r.pipeline() as pipe:
		# Получаем оставшееся время жизни для пользователя и группы через HTTL
		pipe.httl(key, user_id)
		pipe.httl(key, "group")
		results = await pipe.execute()

	# Определяем максимальный TTL из двух значений
	user_ttl, group_ttl = results
	return max(max(user_ttl[0] or 0, group_ttl[0] or 0),0)  # Если TTL не установлен, считаем его равным 0


# Обновляем лимиты
async def upd_daily_limit(chat_id: int, user_id: int, privat:bool):
	key = f"chat:{chat_id}:sum_limits"
	group_field = "group"

	async with r.pipeline() as pipe:
		# Увеличиваем значения лимитов и задаем TTL для каждого поля
		pipe.hincrby(key, user_id)
		pipe.hexpire(key, 86400 // SEND_MES, user_id)  # TTL для пользователя (24 часа)
		
		if not privat:
			pipe.hincrby(key, group_field)
			pipe.hexpire(key, 86400 // SEND_MES_GROUP, group_field)  # TTL для группы (24 часа)
		
		await pipe.execute()


# Запрос к ИИ
async def get_gpt4_summary(text: list, turl: str) -> str:
	#return f"Jlsdgssdgdfhdh\n"	
	# MAX_SUM = auto_gpt_mes_count(count)

	# Задаем системную инструкцию при создании модели
	#первая строка должна быть такой: `--- cut here ---` будет служить для "отрезки лишнего"
	#turl = "t.me/chatname/"
	prompt=f"""
**Роль:** Ты — AI-ассистент, который умеет делать краткие и остроумные пересказы диалогов из Telegram-чатов.

**Задача:** Проанализируй предоставленный фрагмент чата в формате JSON и создай на его основе пост для Telegram в формате Markdown, следуя правилам ниже.

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

Твой пересказ — это крутой пост в TG Markdown.

1.  **Заголовок с эмодзи.** Никаких "Названий темы". Вместо этого — **жирный заголовок и 1-3 подходящих по смыслу эмодзи**.

2.  **Никаких списков "Участники".** Сразу после заголовка — сочный пересказ на 3-7 предложений, **кратко описывающий общую атмосферу и ключевые темы чата**.

3.  **Имена и ссылки на юзеров.**

	  * Используй только имя из поля `full_name` (например, из "Valery Gordienko" делай **Валерий**, из "Анджела Аргунова" — **Анджела**). Следи за правильностью имён, не допускай опечаток (например, пиши "Анджела", а не "Анжела", сверяясь с `full_name`).
	  * При **первом** упоминании пользователя в сводке сделай его имя кликабельной ссылкой, если у него есть `user_name`. Формат: `[Имя](t.me/user_name)`.
	  * Если `user_name` равен `None`, имя пишется просто текстом (можно жирным), без ссылки. Например, для "Vasili Petrovich" с `user_name: None` ты напишешь просто **Василий**.
	  * При **повторных** упоминаниях этого же пользователя в этой же сводке ссылка больше **не нужна**. Имя можно выделить жирным для акцента.
	  * Экранируй спец символы в именах, к примнру Вас_Я приводи к Вас\_Я или исправляй на ВасЯ
	  * Пример: ...а [Иван](t.me/van) в ... или ... попросили [Романа](t.me/Roman) сделать ... или ... упомянув [Лену✨](t.me/Lane) в ...

4.  **Ссылки на сообщения.**

	  * Вместо скучных `[id:...]` делай живую ссылку прямо из ключевой фразы в тексте (1-3 слова).
	  * Формат ссылки: `[ключевая фраза]({turl}id)`. Переменные `id` бери из входного JSON. Не каких разрывов между `](`.
	  * Пример: ...опять сидели бы без [горячей воды](t.me/chatname/452339) в ... или ...пустили новый [слух о ремонте](t.me/chatname/452345) дорог...

5.  **Тон — наше всё!** Пиши так, будто рассказываешь другу, что он пропустил. Используй иронию, подколки, эмодзи и восклицательные знаки, чтобы текст был живым и энергичным.

6.  **Содержание сводки.**

	  * **Фокусируйся на ключевых темах, идеях и выводах.** Избегай дословного пересказа мелких реплик, флуда и шуток, не влияющих на суть.
	  * **Группируй связанные по смыслу сообщения** в один логический блок, даже если они были написаны не по порядку.

7.  **Пределы.**

	  * **Максимальная длина ответа — 3900 символов.** Но стремись к максимальной сжатости без потери смысла. *Less is more*.

**И ОБЯЗАТЕЛЬНО ПОВТОРНО ПРОВЕРЬ перед отправкой:**
1. Длину пересказа (7й пункт требований)
2. Разметку на соответств TG Markdown и экранирование незакрытых символов (`_`, `*`, `[`, `]`, `(`, `)`)
3. Корректность и видимость ссылок (4й пункт требований)
"""

	# await r.lpush('gpt_answ', json.dumps(text).encode('utf-8'))
	# logging.info(text[:50])  # Логируем первые 50 символов текста
	try:
		response = await gclient.aio.models.generate_content(
			model="gemini-2.5-flash",
			config=gtypes.GenerateContentConfig(
				tools=[
					gtypes.Tool(url_context=gtypes.UrlContext()),
				],
				thinking_config=gtypes.ThinkingConfig(thinking_budget=-1),
				system_instruction=prompt,  # Системная инструкция
				),
			contents=json.dumps(text),  # Описание, текст в формате JSON
			)
		return response.text #.rsplit('--- cut here ---', 1)  # Возвращаем текст после "отрезки"

	except Exception as e:
		logging.error(f"Произошла ошибка при обращении к Gemini: {e}", exc_info=True)
		# return f"Ошибка при генерации сводки: {e}"
			

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

########## Чекаем баяны ########
async def get_image_hash(file_id: str) -> str:
	"""Получает dHash изображения по file_id."""
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
		raise RuntimeError(f"Ошибка при получении hash: {e}")

# Функция для вычисления расстояния Хэмминга
async def hamming_distance(hash1, hash2):
	if len(hash1) != len(hash2):
		raise ValueError("Хэши должны быть одинаковой длины")
	return sum(c1 != c2 for c1, c2 in zip(hash1, hash2))

# Функция для поиска похожих хэшей в Redis
async def find_similar_hashes(new_hash, hash_r_key, max_diff=5):	
	try:
		field = await r.hget(hash_r_key,new_hash)
		return json.loads(field)['id']
	except:
		# Получение только полей
		saved_hashes = await r.hkeys(hash_r_key)

		# Сравниваем новый хеш с каждым сохранённым
		for hash in saved_hashes:
				# Если расстояние Хэмминга меньше или равно max_diff, добавляем хэш
				if await hamming_distance(new_hash, hash) <= max_diff:
					return (json.loads(await r.hget(hash_r_key,hash)))['id']
		return None

async def check_bayan(message: Message):
	key = f"chat:{message.chat.id}:bayan"
	chat_id = message.chat.id
	message_id = message.message_id
	bayan = False

	if message.photo:
		file_id = message.photo[0].file_id
	if message.video:
		file_id = message.video.thumbnail.file_id
	
	# Получаем хеш нового изображения
	new_hash = await get_image_hash(file_id)

	id = await find_similar_hashes(new_hash, key, max_diff=BAYANDIFF)
	if id:
		gif_list = (['CgACAgIAAxkBAAIDDmf9qNvELAABV1bhizjsVWVdg_oDYwACjQAD24TBAwNGZWxV-v8LNgQ'], # баба кидает
					['CgACAgQAAxkBAAIDD2f9xJ5wVliPMOyIkLBYFIjVyckiAALEOQAC7hdkB6taFHfHHCwtNgQ'], # в сене находит
					['CgACAgQAAxkBAAIDEGf9xrNV-0FR9CwXnRTzR9as3lOyAALpAgACdgQUUx8P27eBaYgLNgQ'], # Джери
					['CgACAgQAAxkBAAIDEWf9x0Z0QSCD0eWCudKndoLwIHaTAAJSAwAC-RAEU2Tw7_4c3dtnNgQ'], # Пингвин
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
			await message.reply_animation(animation=gif_id, caption=f"Архивная запись №{id}!   [Оригинал тут]({turl})",parse_mode=ParseMode.MARKDOWN)			
		except Exception as e:
			logging.error(f"Нет гивки в чате: {e}")		
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
		model="gemini-2.5-flash",
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

############# Вход и выход из чата ##########
async def kick_msg(Kto: str, Kogo: str, chel: bool) -> str:
	# Извлекаем чистые имена из Markdown-ссылок для передачи в AI
	kto_name_match = re.search(r"\[(.*?)\]", Kto)
	kogo_name_match = re.search(r"\[(.*?)\]", Kogo)
	# Если ссылка не найдена, используем исходную строку (может быть просто имя)
	kto_name = kto_name_match.group(1) if kto_name_match else Kto
	kogo_name = kogo_name_match.group(1) if kogo_name_match else Kogo

	try:
		response = await gclient.aio.models.generate_content(
		model="gemini-2.5-flash",
		contents=f"""
**Задача:** Сгенерируй ОДНО короткое (до 20 слов) и язвительное сообщение-ШАБЛОН о том, что пользователь %%KOGO%% был кикнут пользователем %%KTO%%. И преукрась смайлами.

**Входные данные:**
*   Кикнутый пользователь (Kogo): "{kogo_name}"
*   Тот, кто кикнул (Kto): "{kto_name}"
*   Контекст (is_bot): {chel} (true, если кикнул бот; false, если админ)

**Логика и Грамматика (ОЧЕНЬ ВАЖНО):**
1.  Твоя задача — создать шаблон предложения, используя плейсхолдеры `%%KTO%%` и `%%KOGO%%`.
2.  **Род:** Определи род `Kto` по его имени/нику. Если это женское имя ("Мария", "Нейросеть"), используй глаголы женского рода ("решила", "указала"). Если род неясен (ники вроде "admin", "dv_pod"), используй мужской род по умолчанию.
3.  **Падежи:** Не склоняй плейсхолдеры! Просто вставь `%%KTO%%` и `%%KOGO%%` в нужные места.
4.  **Контекст:** Если `is_bot` это `true`, высмей `Kogo` за то, что его выгнал бездушный бот `Kto`. Если `false`, высмей `Kogo` за то, что его изгнал могущественный админ `Kto`.

**Примеры правильной обработки:**
*   Входные: Kto: 'Мария', Kogo: 'user123' -> Ответ: "%%KTO%% элегантно указала %%KOGO%% на дверь."
*   Входные: Kto: 'dv_pod', Kogo: 'новичок' -> Ответ: "%%KTO%% решил, что %%KOGO%% здесь явно лишний."
*   Входные: Kto: 'Нейросеть', Kogo: 'Спамер' -> Ответ: "%%KTO%% сочла %%KOGO%% цифровым мусором и стерла его."

**Финальное правило:**
Твой ответ должен содержать ТОЛЬКО текст с плейсхолдерами `%%KTO%%` и `%%KOGO%%`. Никакого Markdown.
""",
		)
		# Получаем сгенерированный текст
		generated_text = response.text.strip()

		if not generated_text:
			raise ValueError("Empty response from AI")

		# Заменяем плейсхолдеры на реальные данные (уже с Markdown-ссылками)
		final_text = generated_text.replace("%%KTO%%", Kto).replace("%%KOGO%%", Kogo)
		return final_text
	except Exception as e:
		# Если ИИ не ответил или произошла ошибка, возвращаем стандартное сообщение
		logging.error(f"AI generation failed: {e}")
		return f"👋 {Kto} изгнал(а) пользователя {Kogo}."


# Функция для прогрессивной блокировки
async def get_progressive_ban_duration(chat_id: int, user_id: int) -> (int, str):
	"""
	Вычисляет длительность прогрессивной блокировки для пользователя.
	Увеличивает счетчик банов и возвращает кортеж с:
	1. Временной меткой Unix для окончания бана.
	2. Строкой с описанием срока блокировки.
	"""
	key = f"chat:{chat_id}:ban_counter:{user_id}"
	
	# Увеличиваем счетчик банов для пользователя
	ban_count = await r.incr(key)
	
	# Устанавливаем TTL на 1 год при первом нарушении, чтобы счетчик сбрасывался
	if ban_count == 1:
		await r.expire(key, int(timedelta(days=366).total_seconds()))

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
		
	return int(ban_until.timestamp()), duration_str

# Выход из чата
@dp.chat_member(ChatMemberUpdatedFilter(LEAVE_TRANSITION))
async def off_member(event: ChatMemberUpdated):
    member = event.new_chat_member.user
    logging.info(f"Выход: из чата {event.chat.title} - {member.full_name} {event.new_chat_member}")

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
        kicked_user_link = f"[{html.escape(member.full_name)}]({member.url})"
        admin_user_link = f"[{html.escape(event.from_user.full_name)}]({event.from_user.url})"
        
        # Генерируем и отправляем сообщение о бане (для ручных банов админами)
        kick_message_text = await kick_msg(admin_user_link, kicked_user_link, event.from_user.is_bot)
        await event.answer(kick_message_text, parse_mode="Markdown", disable_web_page_preview=True)

    elif isinstance(event.new_chat_member, ChatMemberLeft):
        # Простое сообщение о том, что пользователь ушел сам (ранее не обрабатывалось)
        await event.answer(
            f"👋 Гудбай [{html.escape(member.full_name)}]({member.url})",
            parse_mode="Markdown", disable_web_page_preview=True)

# Обработчик новых участников
@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def new_member(event: ChatMemberUpdated):
	new_member = event.new_chat_member.user    
	chat_id = event.chat.id
	chat_Ti = event.chat.title
	user_id = new_member.id    
	logging.info(f"Вход! Новенького в чате {chat_Ti} - {new_member.full_name}")

	if not new_member.is_bot:
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
		for sec in range(9, 0, -1): # Начинаем с 9, т.к. 10 уже отправили
			try:
				await asyncio.sleep(1)
				next_message = chosen_template.format(url=new_member.url, name=full_name_html, sec=sec)
				await countdown_msg.edit_text(next_message, parse_mode="HTML")
				# Проверяем статус пользователя на каждом шаге
				member_status = await bot.get_chat_member(chat_id, user_id)
				if member_status.status not in ["member", "restricted"]:
					await countdown_msg.delete()
					# --- НОВАЯ ЛОГИКА ПРОГРЕССИВНОГО БАНА ---
					ban_until_timestamp, duration_str = await get_progressive_ban_duration(chat_id, user_id)
					try:
						await bot.ban_chat_member(
							chat_id=chat_id,
							user_id=user_id,
							until_date=ban_until_timestamp
						)
						logging.warning(f"Пользователь {new_member.full_name} ({user_id}) покинул чат во время отсчета и был забанен на {duration_str}.")
					except Exception as ban_error:
						logging.error(f"Не удалось забанить пользователя {user_id}, покинувшего чат во время отсчета: {ban_error}")
					return
			except Exception as e:
				logging.error(f"Ошибка при обновлении отсчёта или проверке статуса: {e}")
				await countdown_msg.delete()
				return
		# Финальная проверка
		try:
			member_status = await bot.get_chat_member(chat_id, user_id)
			if member_status.status in ["member", "restricted"]:
				await countdown_msg.delete()
				# Сообщение с просьбой картинки (HTML + экранирование)
				full_name_html = (
					f'<a href="{new_member.url}">{html.escape(new_member.full_name)}</a>'
				)
				check_msg = await bot.send_message(
					chat_id,
					f"👋 Привет, {full_name_html}!\n"
					f"Пройди простую проверку.\n\n"
					f"<b>Ответь</b> на <b>это</b> сообщение <u>картинкой велосипеда</u>.\n"
					f"Иначе не сможешь отправлять сообщения 🤐\n"
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
				await countdown_msg.delete()
		except Exception as e:
			logging.error(f"Ошибка при финальной проверке статуса нового пользователя: {e}")
			await countdown_msg.delete()

	else:
		await user_lock_unlock(user_id, chat_id, st="lock")
		visit_message = await bot.send_message(
			chat_id,
			f"Ловите бота!\n"
			f"Звать его - [{new_member.full_name}]({new_member.url})!\n"
			f"Примите или забаньте!\n",
			parse_mode="Markdown"
		)

async def generate_image_description(image: Image.Image) -> str:
	try:
		# Формируем запрос (можно кастомизировать)
		prompt = "Определи есть ли на картинке велосипед и отвть булевым значением True/False"		

		# Генерируем контент асинхронно
		response = await gclient.aio.models.generate_content(
			model="gemini-2.5-flash",
			contents=[image, prompt]
		)

		return eval(response.text) or "Не удалось получить описание. Попробуйте другую картинку"

	except Exception as e:
		logging.error(f"Ошибка в generate_image_description: {e}")
		# В реальном приложении здесь лучше использовать logging
		return f"Ошибка при обращении к Gemini: {e}"

## Ограничиваем возможности пользователя (блокируем отправку сообщений) | расширеный
async def user_lock_unlock(user_id: int, chat_id: int, **kwargs):
	now = datetime.now()
	duration = timedelta(seconds=33)
	future_date = now + duration
	try:
		if kwargs['st'] == 'lock':  # Блокировка
			await bot.restrict_chat_member(
				chat_id=chat_id,
				user_id=user_id,
				permissions=ChatPermissions(
					can_send_photos=True
				),
				until_date=0  # 0 или None – ограничение бессрочное
			)
			logging.info(f"{user_id} - Отправка сообщений заблокирована.")

		elif kwargs['st'] == "unlock":  # Разблокировка

			await bot.restrict_chat_member(
				chat_id=chat_id,
				user_id=user_id,
				permissions=ChatPermissions(can_send_messages=True),
				until_date=future_date)
			logging.info(f"{user_id} - Отправка сообщений разблокирована.")     

	except Exception as e:
		logging.error(f"Error: {e}", exc_info=True)
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
			data = json.loads(data)
			join_time = data["join_time"]
			current_time = int(time.time())
			time_elapsed = current_time - join_time

			if time_elapsed >= NOTIFY_AFTER_SECONDS and not data.get('notified', False):
				user_nm = data.get('full_name', f'user_{user_id}')
				msg_id = data.get('message_id')
				chat_id_str = str(chat_id)[4:] if str(chat_id).startswith('-100') else str(chat_id)
				try:
					reminder = await bot.send_message(
						chat_id=int(chat_id),
						text=f"⏰ [{user_nm}](tg://user?id={user_id}), не забудь пройти проверку!\nОсталось {int((TIME_TO_BAN_SECONDS - time_elapsed)//60)} мин. до 👢💥🍑.\nОтветь на [запрос бота](https://t.me/c/{chat_id_str}/{msg_id})",
						parse_mode="Markdown"
					)
					data['notified'] = True
					data['reminder_id'] = reminder.message_id
					await r.hset(key, user_id, json.dumps(data))
				except Exception as e:
					logging.error(f"Ошибка при отправке уведомления {user_id} в чат {chat_id}: {e}")

			if time_elapsed > TIME_TO_BAN_SECONDS:
				user_nm = data.get('full_name', f'user_{user_id}')
				try:
					# --- НОВАЯ ЛОГИКА ПРОГРЕССИВНОГО БАНА ---
					ban_until_timestamp, duration_str = await get_progressive_ban_duration(int(chat_id), int(user_id))
					await bot.ban_chat_member(
						chat_id=int(chat_id), 
						user_id=int(user_id), 
						until_date=ban_until_timestamp
					)
					logging.info(f"Пользователь {user_id} - {user_nm} забанен на {duration_str} в чате {chat_id} за провал проверки.")
					
					# --- ЯВНАЯ ОТПРАВКА СООБЩЕНИЯ О БАНЕ ---
					bot_user = await bot.get_me()
					bot_link = f"[{html.escape(bot_user.full_name)}]({bot_user.url})"
					kicked_user_link = f"[{html.escape(user_nm)}](tg://user?id={user_id})"
					
					kick_message_text = await kick_msg(bot_link, kicked_user_link, True)
					
					ban_msg = await bot.send_message(
						chat_id=int(chat_id),
						text=kick_message_text,
						parse_mode="Markdown",
						disable_web_page_preview=True
					)
					
					# Запускаем отложенное удаление сообщения о бане
					asyncio.create_task(del_msg_delay(ban_msg, CLEANUP_AFTER_SECONDS))

					# Очищаем данные и сообщения, связанные с проверкой
					await cleanup_verification_data(int(chat_id), int(user_id))
				except Exception as e:
					logging.error(f"Ошибка при исключении {user_id} из чата {chat_id}: {e}")

# приветствие нового участника
@dp.message(Command("hello_m"))
async def cmd_info(message: Message):
	if message.from_user.id in await get_admins(message.chat.id):
		if len(message.text) < 9:
			await message.reply("Напиши текст приветствия после команды /hello_m")
			return
		# Сохраняем сообщение в Redis
		await r.set(f"chat:{message.chat.id}:Hello_msg",message.md_text.split(' ', 1)[1])
		await message.reply(f"🫡")

def escape_markdown_v2(name: str) -> str:
	"""Экранирует специальные символы MarkdownV2 в тексте."""
	# Для простоты, сначала можно экранировать все символы, которые имеют значение в MarkdownV2.
	# Если вы не используете эти символы для форматирования внутри FNAME, то экранирование их будет безопасным.
	# pattern = r"([_*\[\]()~`>#+\-=|{}.!])"
	# return re.sub(pattern, r"\\\1", text)
	chars_to_escape = '_*[]()~`>#+-=|\{\}.!' # Включаем все, что может быть проблемой
	escaped_text = "".join(['\\' + char if char in chars_to_escape else char for char in name])
	return escaped_text

# Слушать сообщения чата
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}), ~F.text.startswith('/')) # Игнорируем команды
async def save_group_message(message: Message):
	logging.info(f"Processing message in chat {message.chat.id}")  # Добавляем логирование
	user = message.from_user
	chat = message.chat
	chat_nm = chat.title
	user_name = user.username or "None"
	full_name = user.full_name or "БезыНя-шка"	

	key = f"chat:{chat.id}:history"
	message_data = {}
	mtext=''
	
	# Проверяем, является ли сообщение ответом на другое сообщение
	if message.reply_to_message and (message.photo or message.text):
		if message.reply_to_message.from_user.is_bot:
			pattern = r'tg://user\?id=(\d+)'
			match = re.search(pattern, message.reply_to_message.md_text)
			if match:
				user_id = match.group(1)
				admins = await get_admins(chat.id)
				key_u_j = f"chat:{chat.id}:new_user_join"
				new_join = await r.hget(key_u_j, user_id)
				if new_join:
					if message.from_user.id == int(user_id):
						try:
							file = await bot.get_file(message.photo[1].file_id)                            
							image_bytes = (await bot.download_file(file.file_path)).read()
							image = Image.open(BytesIO(image_bytes))
							description = await generate_image_description(image)
							
							if description is True:
								# --- РЕШЕНИЕ RACE CONDITION ---
								# Перед отправкой приветствия проверим, не ушел ли пользователь
								try:
									member_status = await bot.get_chat_member(message.chat.id, int(user_id))
									if member_status.status in ("left", "kicked", "banned"):
										logging.warning(f"Пользователь {user_id} покинул чат до завершения проверки. Отменяем приветствие.")
										await cleanup_verification_data(message.chat.id, int(user_id))
										return # Прерываем выполнение
								except Exception:
									logging.warning(f"Не удалось проверить статус {user_id}. Вероятно, он покинул чат. Отменяем приветствие.")
									await cleanup_verification_data(message.chat.id, int(user_id))
									return # Прерываем выполнение

								await user_lock_unlock(user_id, message.chat.id, st="unlock")
								user_obj = await bot.get_chat(chat_id=user_id)
								FNAME = user_obj.full_name or "No_Name"
								FNAME = escape_markdown_v2(FNAME)
								try:
									hell_msg = await r.get(f"chat:{chat.id}:Hello_msg")
									hell_msg = hell_msg.replace('FNAME', FNAME)
								except:
									hell_msg = f"Поприветствуйте {FNAME}, нового участника\\! 👋\n"
								await message.answer(hell_msg,
													 parse_mode=ParseMode.MARKDOWN_V2,
													 disable_web_page_preview=True
													)
								# Централизованная очистка
								await cleanup_verification_data(message.chat.id, int(user_id))
							else:
								answ = "Не удалось найти велосипед 😢" if description else description
								await message.reply(answ)
								# Удаляем фото, если не найден велосипед
								try:
									await bot.delete_message(chat.id, message.message_id)
								except Exception as e:
									if 'message to delete not found' in str(e).lower():
										pass
									else:
										logging.warning(f"Ошибка при удалении фото: {e}")

						except Exception as e:
							logging.error(f"Ошибка при скачивании или обработке фото: {e}")
							await message.reply(f"Не удалось обработать фото. Попробуйте еще раз.\n{e}")
					elif message.from_user.id in admins:
						if "принят" in message.text.lower() or "принят!" in message.text.lower():
							await user_lock_unlock(user_id, message.chat.id, st="unlock")
							user_obj = await bot.get_chat(chat_id=user_id)
							FNAME=user_obj.full_name or "No_Name"
							FNAME = escape_markdown_v2(FNAME)
							try:
								hell_msg = await r.get(f"chat:{chat.id}:Hello_msg")
								hell_msg = hell_msg.replace('FNAME', FNAME)
							except:
								hell_msg = f"Поприветствуйте {FNAME}, нового участника\\! 👋\n"
							await message.answer(hell_msg,
													 parse_mode=ParseMode.MARKDOWN_V2,
													 disable_web_page_preview=True
													)
							# Централизованная очистка
							await cleanup_verification_data(message.chat.id, int(user_id))
						elif "бан" in message.text.lower() or "бан!" in message.text.lower():
							# 1. Применяем прогрессивный бан
							ban_until_timestamp, duration_str = await get_progressive_ban_duration(chat.id, int(user_id))
							await bot.ban_chat_member(
								chat_id=chat.id, 
								user_id=int(user_id), 
								until_date=ban_until_timestamp
							)
							logging.info(f"Администратор {message.from_user.full_name} забанил пользователя {user_id} на {duration_str}.")

							# 2. Генерируем и отправляем сообщение-насмешку
							banned_user_obj = await bot.get_chat(int(user_id))
							banned_user_link = f"[{html.escape(banned_user_obj.full_name)}]({banned_user_obj.url})"
							admin_user_link = f"[{html.escape(message.from_user.full_name)}]({message.from_user.url})"
							
							kick_message_text = await kick_msg(admin_user_link, banned_user_link, False) # is_bot = False
							
							ban_msg = await bot.send_message(chat.id, kick_message_text, parse_mode="Markdown", disable_web_page_preview=True)
							
							# 3. Запускаем отложенное удаление сообщения о бане (10 минут)
							asyncio.create_task(del_msg_delay(ban_msg, CLEANUP_AFTER_SECONDS))

							# 4. Удаляем команду админа ("бан") и очищаем данные
							await message.delete()
							await cleanup_verification_data(message.chat.id, int(user_id))
						else:
							await del_msg_delay(await message.reply("Можно только 'Принят!' или 'Бан!'."))
				else:
					await del_msg_delay(await message.reply("Уже всё, поздно 😏"))

	# База баянов
	bayan = False
	if message.photo or message.video:
		bayan = await check_bayan(message)

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
	
	# Обновляем счетчики в Redis
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

	logging.info(f"Message {message.message_id} saved and stats updated for period {current_period} in {chat_nm}")


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