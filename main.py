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
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import redis.asyncio as redis
from dotenv import load_dotenv
from os import getenv
import random

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command, ChatMemberUpdatedFilter, JOIN_TRANSITION, LEAVE_TRANSITION
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import ChatMemberAdministrator, ChatMemberOwner, ChatMemberRestricted, ChatMemberLeft, \
	ChatMemberBanned, ChatMemberMember, ChatPermissions, ChatMemberUpdated, InlineKeyboardMarkup, InlineKeyboardButton, \
	Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, BufferedInputFile

from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import imagehash
import google.generativeai as genai

# Настройка базовой конфигурации логирования
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', 
					level=logging.INFO)

# Загрузка конфигурации
load_dotenv()
ADMIN_ID = getenv("ADMIN_ID")
TELEGRAM_TOKEN = getenv("TELEGRAM_TOKEN")

# Настройка Gemini
GOOGLE_API_KEY = getenv("GOOGLE_API_KEY")
genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

#RAPIDAPI_KEY = getenv("RAPIDAPI_KEY")
REDIS_HOST = getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(getenv("REDIS_PORT", 6379))

# Конфигурация лимитов
LIMIT_SECONDS = 5

ADMIN_IDS = {ADMIN_ID}
LOCK_FOR_SUMMARIZE = set()
MAX_HISTORY = 10000  # Всего сообщений истории
MAX_TO_GPT = 2000  # Сообщений в гпт
MIN_TO_GPT = 150
MAX_SUM = 3500  # сивлолов для ответа суммар
DEF_SUM_MES = 200  # дефолтно для суммаризации
SEND_MES = 2  # число запросов в 24ч
SEND_MES_GROUP = 5  # число запросов в 24ч

VERIFICATION_REM = 1  # час
BAN_AFTER_HOURS = 20  # часов до бана

BAYANDIFF = 5 # разница хешей картинок

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
async def cmd2(message: types.Message):
	await message.answer("1")


# Проверка прав
@dp.message(Command("right"))
async def get_perm(message: types.Message):
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
async def summarize(message: Message):
	logging.info(f"Команда summarize вызвана в чате: {message.chat.title}")
	if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
		await message.answer("Эта команда работает только в групповых чатах!")
		return

	user_id = message.from_user.id
	user_nm = message.from_user.username or 'БезыНЯ-шка'
	chat_id = message.chat.id
	chat_Ti = message.chat.title

	# Проверка на доступ у чата и юзера
	req = await is_user_approved(chat_id, user_id)

	if req == 'on':
		# 1. Проверяем, занят ли чат
		if chat_id in LOCK_FOR_SUMMARIZE:
			await message.reply("ХарЭ столько тыкать. Пожалуйста, подождите.")
			return # Прекращаем выполнение, если чат занят

		# 2. Добавляем чат в список обрабатываемых (захватываем блокировку)
		LOCK_FOR_SUMMARIZE.add(chat_id)
		params = message.text.split()[1:]  # Получаем все параметры после команды
		num_messages = 0
		offset = 0

		try:
			if len(params) >= 1:
				num_messages = int(params[0])
				num_messages = max(MIN_TO_GPT, min(num_messages, MAX_TO_GPT))
			if len(params) >= 2:
				offset = int(params[1])

			await process_summarize(message, num_messages, offset)

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
async def process_summarize(message: Message, count=0, start=0):
	chat = message.chat
	chat_id = chat.id
	chat_nm = chat.username or chat.title
	user = message.from_user
	new_messages = []
	msg_old_id = None	

	ttl = await check_daily_limit(chat_id, user.username)
	#ttl = 9990
	if (ttl > 0): # and (user.id not in ADMIN_IDS):
		await del_msg_delay(await message.answer(f"❌ Достигнут лимит запросов!\n Подождите {format_seconds(ttl)}"))
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
	try:
		await bot.send_chat_action(chat_id, action="typing")
		logging.info(f"передача 1msg-{messages[0]}\n"
			   		f"число-{count}, last_sum_id-{surl}, msg_old_id-{msg_old_id}")
		summary = await get_gpt4_summary(messages, turl)
		logging.info(f"Ответ gpt4: получен. Длина {len(summary)}")
		
		sum = await message.answer(f"📝 #Суммаризация последних {count} сообщений:\n{summary}" \
								   f"\n{surl}",
									disable_web_page_preview=True, ## убираем превью
									parse_mode=ParseMode.MARKDOWN  # Используй ParseMode.MARKDOWN
									)

	except Exception as e:
		logging.error(f"GPT error: {e}")
		await message.answer("Ошибка получения данных от GPT")
		return	
		
	# Обновляем дневной лимит
	await upd_daily_limit(chat_id, user.username)
	
	if new_messages:
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
async def upd_daily_limit(chat_id: int, user_id: int):
	key = f"chat:{chat_id}:sum_limits"
	group_field = "group"

	async with r.pipeline() as pipe:
		# Увеличиваем значения лимитов и задаем TTL для каждого поля
		pipe.hincrby(key, user_id)
		pipe.hexpire(key, 86400 // SEND_MES, user_id)  # TTL для пользователя (24 часа)
		
		pipe.hincrby(key, group_field)
		pipe.hexpire(key, 86400 // SEND_MES_GROUP, group_field)  # TTL для группы (24 часа)
		
		await pipe.execute()


# Запрос к ИИ
async def get_gpt4_summary(text: str, turl: str) -> str:
	#return f"Jlsdgssdgdfhdh\n"	
	# MAX_SUM = auto_gpt_mes_count(count)

	# Задаем системную инструкцию при создании модели
	prompt=f"""	Ты - опытный и остроумный пересказчик событий из Telegram-чата.
	Сделай стиль рассказа лёгким и неформальным, отражающим дружелюбную атмосферу чата. Добавляй эмодзи, чтобы разбавить текст.
	Твоя задача:
	 — Прочитать поток сообщений.
	 — Выделить несколько ключевых тем, вынеся в заголовки (используй * для заголовков) и расскажи что происходило
	При этом нужно учитывать следующие моменты:
	- Каждое сообщение представлено в формате JSON с полями "id", "reply_to" (может отсутствовать), "user_name", "full_name", и "text".
	- Поле "reply_to" содержит id сообщения, на которое данное сообщение является ответом (если это ответ). Отсутствие поля означает, что сообщение является ответом на предыдущее (с меньшим id), но нужно смотреть контекст.
	- Всегда упоминай пользователей по полному имени (поле "full_name").
	  - Если user_name не равен "non", оформляй имя пользователя как [full_name](t.me/user_name).
	  - Если user_name равен "non" или "БезыНя-шка", упоминай пользователя только по полному имени: full_name.
	- Важные моменты/фразы из сообщений пересказывай и оформляй как кликабельные ссылки на соответствующие сообщения. Используй стандартный Markdown формат [текст ссылки](URL).
	- Текст ссылки: краткая фраза или слово из сообщения, которое ты пересказываешь.
	- URL: полный адрес сообщения в формате {turl}id, где {turl} - базовая часть URL чата (передается тебе отдельно), а id - значение поля "id" из JSON-сообщения.
	Пример правильного формата: [объявил о начале]({turl}397) - здесь в скобках `()` ТОЛЬКО URL.
	
	И добвавь в конце "Если пропустил несколько сообщений — не страшно. Здесь обсуждали" продолжив фразу
	И ещё одно правило, нельзя превышать 3700 символов, где каждая буква 1 символ и смайлик это всего 4 символа
	"""

	await r.lpush('gpt_answ', json.dumps(text))
	
	model = genai.GenerativeModel(
		model_name='gemini-2.0-flash', # Или другая модель
		# Задаем системную инструкцию при создании модели
		system_instruction=prompt
	)

	try:      
		# Генерируем контент асинхронно
		response = await model.generate_content_async({
				'mime_type': 'text/plain',  # Указываем mime_type
				'data': json.dumps(text).encode('utf-8')
			})

		return response.text or None

	except Exception as e:
		logging.error(f"Ошибка в gpt generate_content_async: {e}")
		return None, f"Ошибка при обращении к Gemini: {e}"
			

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
		contents = f"Придумай короткий анекдот на тему: {tema}"
	else:
		key = f"chat:{message.chat.id}:history"
		txt = await r.lrange(key, 0, 30)
		txt = [json.loads(message_json)['text'] for message_json in txt]
		txt.reverse()
		contents = f"Придумай короткий анекдот по событиям данной переписки: {txt}"	
	
	# Генерируем контент асинхронно
	response = await model.generate_content_async(contents=contents)
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
async def get_top_users(message: Message, count: int = 10, table_text=None) -> list:
	chat_id = message.chat.id
	arg = message.text[6:].split()

	if len(arg) > 1 and arg[1] is not None:
		try:
			count = int(arg[1])
		except ValueError:
			await message.answer('Вторым значением должно быть число топа (по дефолту 10)')

	if len(arg) == 0 or arg[0] == 'm':
		key = f"chat:{chat_id}:count_u_msg"
		title = f"Топ {count} отправлятелей сообщений"
	elif arg[0] == 'c':
		key = f"chat:{chat_id}:count_u_len"
		title = f"Топ {count} печатателей буков"
	elif arg[0] == 'b':
		key = f"chat:{chat_id}:count_u_byn"
		title = f"Топ {count} баянистов"
	else:
		await message.answer('Всё не так! Нудно команда и через пробел параметры - буква топа (m,c,b), число топа (по дефолту 10)')
		return

	top_users = await r.zrevrange(key, 0, count - 1, withscores=True)
	result = []
	for user_id_bytes, score in top_users:
		user_id = user_id_bytes
		score = int(score)
		# Получаем имя пользователя через ChatMember
		try:
			member = await bot.get_chat_member(chat_id, int(user_id))
			name = member.user.full_name or "No_Name"
		except:
			name = "Unknown"
		result.append((user_id, score, name))	
	
	if len(arg) > 2 and arg[2].lower() == 't':
		# Константы для максимальной длины колонок
		MAX_LEN = 30
		MAX_PLACE_LEN = 2  # Для медалек или номеров (1-99)
		MAX_SCORE_LEN = 6  # Например, "1.23M", "99.9k", "9999"

		# Вычисляем оставшееся место для имени: общая длина - место - пробел - счет - пробел
		MAX_DISPLAY_NAME_LEN = MAX_LEN - MAX_PLACE_LEN - 1 - MAX_SCORE_LEN - 1 
		# Формируем текст-таблицу
		table_text = f"<b>{title}</b>\n" # Заголовок жирным
		
		# Начало моноширинного блока
		table_text += "<pre>\n"

		# Добавляем строки с данными
		for i, item in enumerate(result):
			if i == 0:
				place = "🥇"
			elif i == 1:
				place = "🥈"
			elif i == 2:
				place = "🥉"
			else:
				place_num = str(i + 1)
				if len(place_num) > MAX_PLACE_LEN:
					place = ".."
				else:
					place = str(i + 1).ljust(MAX_PLACE_LEN)

			if len(item[2]) > MAX_DISPLAY_NAME_LEN:
				# Если имя слишком длинное, обрезаем его
				name = item[2][:MAX_DISPLAY_NAME_LEN - 2] + ".."
			name = item[2].ljust(MAX_DISPLAY_NAME_LEN)
			score = humanize_value_for_chars(item[1]).ljust(MAX_SCORE_LEN)
			table_text += f"{place} {name} {score}\n"
		# Закрываем моноширинный блок
		table_text += "</pre>\n"

		# Отправляем сообщение с текстом-таблицей
		await message.answer(table_text, parse_mode='HTML')

	else: # output_type == "image"
		"""Отправляет изображение топа в чат."""
		image_data = await generate_pil_image(result, title)
		photo = BufferedInputFile(image_data.getvalue(), filename="top.png")
		await bot.send_photo(chat_id, photo=photo)
	
def humanize_value_for_chars(num: int) -> str: # Переименовано
	"""Преобразует количество символов в человекочитаемый формат."""
	if num >= 1000000:
		return f"{num / 1000000:.2f}M"
	elif num >= 1000:
		return f"{num / 1000:.2f}K"
	else:
		return str(num)

############# Проверка на бота по Картинке ##########
# Выход из чата
@dp.chat_member(ChatMemberUpdatedFilter(LEAVE_TRANSITION))
async def off_member(event: ChatMemberUpdated):
	member = event.new_chat_member.user
	logging.info(f"Выход:\n из чата {event.chat.title} - {member.full_name}")
	member = event.new_chat_member.user
	await event.answer(
		f"👋 Гудбай [{member.full_name}]({member.url})",
		parse_mode="Markdown")

# Обработчик новых участников
@dp.chat_member(ChatMemberUpdatedFilter(JOIN_TRANSITION))
async def new_member(event: ChatMemberUpdated):    
	new_member = event.new_chat_member.user    
	chat_id = event.chat.id
	chat_Ti = event.chat.title
	user_id = new_member.id    
	logging.info(f"Вход!\n Новенького в чате {chat_Ti} - {new_member.full_name}")

	if not new_member.is_bot:
		# Ограничиваем права нового участника
		await user_lock_unlock(user_id,chat_id, st="lock")
		# Отправляем приветственное сообщение
		visit_message = await bot.send_message(
			chat_id,
			f"👋 Привет, [{new_member.full_name}]({new_member.url})!\n"
			f"Пройди простую проверку\n"
			f"Ответь на это сообщение картинкой велосипеда.\n"
			f"А то ты не сможешь отправлять сообщения 🤐.\n",
			parse_mode="Markdown"
			)
		# Сохраняем узера месагу и время присоединения
		await r.hset(f"chat:{chat_id}:new_user_join",new_member.id,
			json.dumps({
				"message_id": visit_message.message_id,
				"full_name": new_member.full_name,
				"join_time": int(time.time())
			})
		)
	else:
		await user_lock_unlock(user_id,chat_id, st="lock")
		visit_message = await bot.send_message(
			chat_id,
			f"Ловите бота!\n"
			f"Звать его - [{new_member.full_name}]({new_member.url})!\n"
			f"Примите или забаньте!\n",
			parse_mode="Markdown"
			)

async def generate_image_description(image_bytes: bytes) -> str:
	try:
	# Подготавливаем данные изображения для API
		image_part = {
			"mime_type": "image/jpeg", # Предполагаем JPEG для фото из Telegram
			"data": image_bytes
		}
		# Формируем запрос (можно кастомизировать)
		prompt = "Определи есть ли на картинке велосипед и отвть булевым значением True/False"
		contents = [prompt, image_part]

		# Генерируем контент асинхронно
		response = await model.generate_content_async(contents=contents)
		return eval(response.text) or "Не удалось получить описание. Попробуйте другую картинку"

	except Exception as e:
		logging.error(f"Ошибка в generate_image_description: {e}")
		# В реальном приложении здесь лучше использовать logging
		return f"Ошибка при обращении к Gemini: {e}"

## Ограничиваем возможности пользователя (блокируем отправку сообщений) | расширеный
async def user_lock_unlock(user_id: int, chat_id: int, **kwargs):
	now = datetime.now()
	duration = timedelta(seconds=60)
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
async def check_new_members():
	TIME_BAN=60*60*24 # 24 часа
	# Сканируем все ключи в Redis
	async for key in r.scan_iter("chat:*:new_user_join"):
		# Извлекаем chat_id из ключа (ключ имеет вид chat:<chat_id>:new_user_join)
		chat_id = key.split(":")[1]
		# Получаем все поля и значения для данного чата
		members = await r.hgetall(key)
		for user_id, data in members.items():
			# Данные уже строка, так как decode_responses=True
			data = json.loads(data)
			join_time = data["join_time"]
			current_time = int(time.time())
			if current_time - join_time > TIME_BAN:
				try:
					# Исключаем пользователя
					await bot.ban_chat_member(chat_id=int(chat_id), user_id=int(user_id), until_date=current_time + TIME_BAN)
					# Удаляем данные из Redis
					await r.hdel(key, user_id)
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
	chars_to_escape = '_*[]()~`>#+-=|{}.!' # Включаем все, что может быть проблемой
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
							description = await generate_image_description(image_bytes)
							# await message.answer(f"otvet - {description}")
							if description==True:
								await user_lock_unlock(user_id, message.chat.id, st="unlock")
								user_obj = await bot.get_chat(chat_id=user_id)
								FNAME = user_obj.full_name or "No_Name"
								FNAME = escape_markdown_v2(FNAME)
								try:
									hell_msg = await r.get(f"chat:{chat.id}:Hello_msg")
									hell_msg = hell_msg.replace('FNAME', FNAME)
								except:
									hell_msg = f"Поприветствуйте {FNAME}, нового участника\! 👋\n"
								await message.answer(hell_msg,
                                                     parse_mode=ParseMode.MARKDOWN_V2,
                                                     disable_web_page_preview=True
                                                    )
								await r.hdel(key_u_j, user_id)
							else:
								answ = "Не удалось найти велосипед 😢" if description else description
								await message.reply(answ)

						except Exception as e:
							logging.error(f"Ошибка при скачивании или обработке фото: {e}")
							await message.reply(f"Не удалось обработать фото. Попробуйте еще раз.\n{e}")
					elif message.from_user.id in admins:
						if message.text == "Принят!":
							await user_lock_unlock(user_id, message.chat.id, st="unlock")
							user_obj = await bot.get_chat(chat_id=user_id)
							FNAME=user_obj.full_name or "No_Name"
							try:
								hell_msg = await r.get(f"chat:{chat.id}:Hello_msg")
								hell_msg = hell_msg.replace('FNAME', FNAME)
							except:
								hell_msg = f"Поприветствуйте {FNAME}, нового участника! 👋\n"
							await message.answer(hell_msg)
							await r.hdel(key_u_j, user_id)
						elif message.text == "Бан!":
							await bot.ban_chat_member(chat.id, user_id)
							await message.reply("Пользователь заблокирован.")
							await r.hdel(key_u_j, user_id)
						else:
							await message.reply("Можно только 'Принят!' или 'Бан!'.")
				else:
					await message.reply("Уже всё, поздно 😏")

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
	await r.zincrby(f"chat:{chat.id}:count_u_msg", 1, user.id)
	await r.zincrby(f"chat:{chat.id}:count_u_len", len(message_data['text']), user.id)
	if bayan: await r.zincrby(f"chat:{chat.id}:count_u_byn", 1, user.id)
			
# save in db
	try:
		async with r.pipeline() as pipe:
			await pipe.lpush(key, json.dumps(message_data))
			await pipe.ltrim(key, 0, MAX_HISTORY - 1)
			await pipe.execute()
	
		logging.info(f"Message {message.message_id} saved successfully in {chat_nm}")
	except Exception as e:
		logging.error(f"Error saving message: {e}", exc_info=True)

# Настройка планировщика
def setup_scheduler():
	scheduler = AsyncIOScheduler()
	scheduler.add_job(check_new_members, 'interval', minutes=60) 
	scheduler.start()


# Запуск бота
async def main():
	print("✅ Бот запущен!")
	# setup_scheduler() # Настройка планировщика
	await init_redis()
	await dp.start_polling(bot)

if __name__ == "__main__":
	asyncio.run(main())