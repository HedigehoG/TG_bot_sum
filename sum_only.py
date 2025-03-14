"""Начальная настройка!
В BotFather с помощью команды /setprivacy, необходимо выключить "Disabled" (Выключено) Режим конфиденциальности (Privacy Mode)
Создать .env файл с одноименными параметрами TELEGRAM_TOKEN=12345, и т.д. """

import asyncio
import logging
import json
import uuid
from datetime import datetime, timedelta

import aiohttp
from aioredis import Redis
from dotenv import load_dotenv
from os import getenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import ChatMemberAdministrator, ChatMemberOwner, ChatMemberRestricted, ChatMemberLeft, ChatMemberBanned, ChatMemberMember
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery


# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(level=logging.INFO)

# Загрузка конфигурации
load_dotenv()
ADMIN_ID = int(getenv("ADMIN_ID", 123456))
TELEGRAM_TOKEN = getenv("TELEGRAM_TOKEN")
RAPIDAPI_KEY = getenv("RAPIDAPI_KEY")
REDIS_HOST = getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(getenv("REDIS_PORT", 6379))

# Конфигурация лимитов
LIMIT_SECONDS = 5
WHITELIST = {getenv("ADMIN_ID"), 987654321}

ADMIN_IDS = {}
MAX_HISTORY = 10000 # Всего сообщений истории
MAX_TO_GPT = 2000 # Сообщений в гпт
MAX_SUM = 3000 # сивлолов для ответа суммар
DEF_SUM_MES = 200 # дефолтно для суммаризации
SEND_MES = 2 # число запросов в 24ч
SEND_MES_GROUP = 5 # число запросов в 24ч

VERIFICATION_REM = 1 # час
BAN_AFTER_HOURS = 20 # часов до бана


# Инициализация 
bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
dp["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")


## Проверка настройки Privacy Mode
async def main_aio():  
	bot_info = await bot.get_me()    
	if bot_info.can_read_all_group_messages:
		print("Режим конфиденциальности выключен: бот может читать все сообщения в группе.")
	else:
		print("Режим конфиденциальности включен: бот имеет ограниченный доступ к чтению в группах.")

# Инициализация подключения к Redis
r = None
async def init_redis():
	global r
	try:
		r = await Redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}", decode_responses=True)
		logging.info("Connected to Redis successfully")
	except Exception as e:
		logging.error(f"Failed to connect to Redis: {e}")

# Начальная команда Start
@dp.message(CommandStart())
async def start(message: Message):
	if message.chat.type == ChatType.PRIVATE:
		await message.answer(
			#"Привет! Я бот для суммаризации чатов. Добавь меня в группу и используй /summarize"
			"Привет! Я бот для суммаризации чатов, проверьте права /right"
		)

#Проверка времени работы
@dp.message(F.text, Command("run_info"))
async def cmd_info(message: Message, started_at: str):    
	await message.answer(f"Бот запущен {started_at}")

# тест комады 
@dp.message(Command("t2"))
async def cmd2(message: types.Message):
	await message.answer("1")

# Проверка прав
@dp.message(Command("right"))
async def get_perm(message: types.Message):
	try:
		user_id = message.reply_to_message.from_user.id
	except Exception as e:
		user_id = bot.id
		await main_aio()
	chat_id = message.chat.id
	logging.info(f"Check right {user_id}")
	has_permission, message = await check_admin_permissions(chat_id, user_id)
	print(f"Проверка прав администратора: {message}")

# Обработка комады summarize
@dp.message(Command("summarize"))
async def summarize(message: Message):
	logging.info(f"summarize {message.chat.type}")
	if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
		await message.answer("Эта команда работает только в групповых чатах!")
		return

	user_id = message.from_user.id
	chat_id = message.chat.id
	chat_Ti = message.chat.title
	
	# Проверка на доступ у чата и юзера
	if await is_user_approved(chat_id, user_id):
		try:
			num_messages = int(message.text.split()[1]) if len(message.text.split()) > 1 else DEF_SUM_MES
			num_messages = max(10, min(num_messages, MAX_TO_GPT))
		except ValueError:
			await message.answer(f"Некорректное число сообщений. Использую {DEF_SUM_MES}.")
			num_messages = DEF_SUM_MES
		
		return await process_summarize(message, num_messages)
		
	request_id = await create_approval_request(user_id, chat_id, chat_Ti)
	keyboard = InlineKeyboardMarkup(inline_keyboard=[
		[InlineKeyboardButton(text="✅ Одобрить", callback_data=f"approve:{request_id}:{chat_id}")],
		[InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject:{request_id}:{chat_id}")]
	])
	
	await bot.send_message(
		ADMIN_ID,
		f"⚠️ Новый запрос на доступ:\n"
		f"User: {message.from_user.full_name} (@{message.from_user.username})\n"
		f"Группа: {chat_Ti}\n"
		f"ID запроса: {request_id}",
		reply_markup=keyboard
	)
	
	await message.reply(
		"❌ Вам пока не доступна эта функция.\n"
		"Запрос на одобрение отправлен администратору."
	)

def format_seconds(seconds):
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    if hours > 0:
        return f"{hours}ч:{minutes:02d} мин."  # :02d добавляет ведущий ноль к минутам
    else:
        return f"{minutes} мин."

# чекаем в бд
async def is_user_approved(chat_id: int ,user_id: int) -> bool:
	return bool(await r.hget(f"chat:{chat_id}:sum_run_v", user_id))

# Делаем суммаризацию
async def process_summarize(message: Message, num_messages: int):
	chat = message.chat
	chat_id = chat.id
	chat_nm = chat.username or chat.title
	user = message.from_user
	
	ttl = await check_daily_limit(user.username,chat_id)
	if ( ttl > 0):
		await message.answer(f"❌ Достигнут лимит запросов!\n Подождите {format_seconds(ttl)}")
		return
	
	try:
		key = f"chat:{chat_id}:history"
		logging.info(f"Строка подключения {key}")
		messages = await r.lrange(key, 0, num_messages - 1)
		messages = messages[::-1]
	except Exception as e:
		logging.error(f"Redis history error: {e}")
		await message.answer("Ошибка получения истории чата")
		return

	if not messages:
		await message.answer("Нет сообщений для суммаризации.")
		return

	chat_text = "Краткая суммаризация чата:\n" + "\n".join(messages)
	chat_id_str = str(chat_id)[4:] ## обрезка индекса для ссылки на чат
	chat = message.chat
	if chat.username:
		turl = f"t.me/{chat.username}/"
	else:
		turl = f"t.me/c/{chat_id_str}/"
	try:
		await bot.send_chat_action(chat_id, action="typing")
		summary = await get_gpt4_summary(chat_text, turl)
		logging.info(f"Ответ gpt4:\n {summary}")
		await message.answer(f"📝 Суммаризация последних {num_messages} сообщений:\n{summary}", 
		disable_web_page_preview=True) ## убираем превью
		# Обновляем дневной лимит
		await upd_daily_limit(user.username,chat_id)
	except Exception as e:
		logging.error(f"GPT error: {e}")
		await message.answer("Ошибка обработки запроса")

# ставим в бд status pending
async def create_approval_request(user_id: int, chat_id: int, chat_Ti: str) -> str:
	request_id = str(uuid.uuid4())
	request_data = {
		"user_id": user_id,
		"chat_id": chat_id,
		"chat_Ti": chat_Ti,
		"timestamp": datetime.now().isoformat(),
		"status": "pending"
	}
	async with r.pipeline() as pipe:
		await pipe.hset(f"chat:{chat_id}:sum_request", request_id, json.dumps(request_data))
		await pipe.expire(f"chat:{chat_id}:sum_request", 86400)
		await pipe.execute()
	#await r.hset(f"chat:{chat_id}:sum_request", {request_id}, json.dumps(request_data), ex=86400)
	return request_id
	
# чекаем в бд дневной лимиты
async def check_daily_limit(user_id: int,chat_id: int) -> int:
	keyU = f"chat:{chat_id}:sum_run:{user_id}"
	keyG = f"chat:{chat_id}:sum_run"
	ttl = max(int(await r.ttl(keyU)),int(await r.ttl(keyG)))       
	return ttl

# Обновляем лимиты
async def upd_daily_limit(user_id: int,chat_id: int) -> int:
	keyU = f"chat:{chat_id}:sum_run_u:{user_id}"
	keyG = f"chat:{chat_id}:sum_run"
	await r.incr(keyU)
	await r.expire(keyU, 86400 // SEND_MES) ## 24ч делим на сообщения юзера
	await r.incr(keyG)
	await r.expire(keyG, 86400 // SEND_MES_GROUP) ## 24ч делим на сообщения чата

# Запрос к ИИ
async def get_gpt4_summary(text: str, turl: str) -> str:    
	#########################################
	print(f"Roman упомянул [тема]({turl}/message_id)")
	return "Jlsdgssdgdfhdh"
	########################################
	async with aiohttp.ClientSession() as session:
		async with session.post(
			url="https://chatgpt-42.p.rapidapi.com/gpt4",
			headers={
				"Content-Type": "application/json",
				"X-RapidAPI-Key": RAPIDAPI_KEY,
				"X-RapidAPI-Host": "chatgpt-42.p.rapidapi.com"
			},
			json={
				"messages": [{
					"role": "user",
					"content": f"Создай краткую сводку приведенных сообщений чата (не более {MAX_SUM} символов), включая в сам текст ссылки на основные темы в формате (и ранней истории, при наличии): Роман (@Hedgehog) упомянул <a href='{turl}43'>тема</a>:\n{text}, если пользователь упоминется не часто, то никнейм можно не указывать."
				}]
			}
		) as response:
			response.raise_for_status()
			data = await response.json()
			return data.get("result", "Не удалось получить суммаризацию")
			
# Обработка подтверждения/отказа запроса
@dp.callback_query(F.data.startswith("approve:") | F.data.startswith("reject:"))
async def handle_approval(callback: CallbackQuery):
	logging.info(f"approval callback_query {callback.data}")
	action, request_id, chat_id = callback.data.split(":")      
	
	request_data = json.loads(await r.hget(f"chat:{chat_id}:sum_request", request_id) or "{}")
	
	if not request_data:
		return await callback.answer("Запрос не найден!")
	
	if action == "approve":
		#await r.incr(f"chat:{chat_id}:sum_run_v")
		await r.hset(f"chat:{chat_id}:sum_run_v", request_data['user_id'], 1)
		await callback.message.edit_text(f"✅ Запрос {request_id} одобрен!")
		
		await bot.send_message(
			request_data['user_id'],
			f"🎉 Вам одобрен доступ к команде /summarize в группе {request_data['chat_Ti']}!"
		)
	else:
		await r.hset(f"chat:{chat_id}:sum_run_v", request_data['user_id'], 0)
		await callback.message.edit_text(f"❌ Запрос {request_id} отклонен!")
		await bot.send_message(
			request_data['user_id'],
			f"🚫 Ваш запрос на доступ к /summarize был отклонен."
		)
	
	await callback.answer()

# Слушать сообщения чата
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def save_group_message(message: Message, bot: Bot):
	"""Сохраняет сообщения из группы, игнорируя команды бота и проверяя права."""
	if message.text and message.text.startswith('/'):
		logging.info(f"Словили команду {message.text}")
		return  # Игнорируем команды бота
	
	logging.info(f"Processing message in chat {message.chat.id}") # Добавляем логирование
	
	if message.text:
		try:
			user = message.from_user
			chat = message.chat
			chat_nm = chat.username or chat.title
			user_name = user.username or "Anonyst"
			full_name = user.full_name or "Unknown User"
			
			await add_message(
				chat_id=chat.id,
				chat_nm=chat_nm,
				message_id=message.message_id,
				user_name=user_name,
				full_name=full_name,
				message_text=message.text
			)
			logging.info("Message saved successfully")
		except Exception as e:
			logging.error(f"Error saving message: {e}", exc_info=True)

# сохранение сообщений чата в бд
async def add_message(chat_id: int, chat_nm: str, message_id: int, user_name: str, full_name: str, message_text: str):
	logging.info(f"Add message from {full_name} ({user_name}) in chat {chat_nm}")
	try:
		if not message_text.strip():
			return        
		key = f"chat:{chat_id}:history"
		message_data = {
			'id': message_id,
			'user_name': user_name,
			'full_name': full_name,
			'text': message_text.strip()[:4096]
		}
		async with r.pipeline() as pipe:
			await pipe.lpush(key, json.dumps(message_data))
			await pipe.ltrim(key, 0, MAX_HISTORY - 1)
			await pipe.execute()
	except Exception as e:
		logging.error(f"Redis error: {e}")
			
# Проверка прав бота/Узера
async def check_admin_permissions(chat_id, user_id):
	chat_member = await bot.get_chat_member(chat_id, user_id)
	logging.info(f"check_admin_permissionschat_member {chat_member}")
	if isinstance(chat_member, ChatMemberOwner):
		return True, "Бот является владельцем и имеет все разрешения."
	elif isinstance(chat_member, ChatMemberAdministrator):
		if (
			chat_member.can_delete_messages
			and chat_member.can_restrict_members
			and chat_member.can_pin_messages
			and chat_member.can_manage_topics
		):
			return True, "Бот является администратором с правом удалять сообщения, блокировать участников, закреплять сообщения или управлять темами."
		else:
			return (
				False,
				"Бот является администратором, но не имеет достаточных прав для чтения сообщений (нет прав на удаление, блокировку, закрепление или управление темами).",
			)
	elif isinstance(chat_member, ChatMemberRestricted):
		return False, "Бот ограничен в правах и не может читать все сообщения."
	elif isinstance(chat_member, ChatMemberLeft):
		return False, "Бот покинул чат и не может читать сообщения."
	elif isinstance(chat_member, ChatMemberBanned):
		return False, "Бот заблокирован в чате и не может читать сообщения."
	elif isinstance(chat_member, ChatMemberMember):
		return (
			False,
			"Бот является обычным участником и не может читать все сообщения при включенном режиме конфиденциальности.",
		)
	else:
		return False, f"Неизвестный статус: {type(chat_member)}"          

# Запуск бота
async def main():
	await init_redis()
	await dp.start_polling(bot)

if __name__ == "__main__":    
	asyncio.run(main())