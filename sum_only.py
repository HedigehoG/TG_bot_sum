"""Начальная настройка!
В BotFather с помощью команды /setprivacy, необходимо выключить "Disabled" (Выключено) Режим конфиденциальности (Privacy Mode)
Создать .env файл с одноименными параметрами TELEGRAM_TOKEN=12345, и т.д. """

import asyncio
import logging
import json
import uuid
from datetime import datetime

import aiohttp
#import redis

import redis.asyncio as redis  # Изменено
#from aioredis import Redis
from dotenv import load_dotenv
from os import getenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import CommandStart, Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatType
from aiogram.types import ChatMemberAdministrator, ChatMemberOwner, ChatMemberRestricted, ChatMemberLeft, \
	ChatMemberBanned, ChatMemberMember
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

import debugpy

# debugpy.listen(('0.0.0.0', 5678))

# print("Waiting for debugger attach")
#debugpy.wait_for_client()


# Включаем логирование, чтобы не пропустить важные сообщения
logging.basicConfig(
	level=logging.INFO,
	format='%(asctime)s - %(levelname)s - %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S',  # Формат даты и времени
	handlers=[
		#logging.FileHandler('/var/log/t-bot.out'),
		logging.StreamHandler()
	]
)

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

ADMIN_IDS = {ADMIN_ID}
MAX_HISTORY = 10000  # Всего сообщений истории
MAX_TO_GPT = 2000  # Сообщений в гпт
MIN_TO_GPT = 100
MAX_SUM = 3500  # сивлолов для ответа суммар
DEF_SUM_MES = 200  # дефолтно для суммаризации
SEND_MES = 2  # число запросов в 24ч
SEND_MES_GROUP = 5  # число запросов в 24ч

VERIFICATION_REM = 1  # час
BAN_AFTER_HOURS = 20  # часов до бана

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
async def cmd_info(message: Message, started_at: str):
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
@dp.message(Command("summarize"))
async def summarize(message: Message):
	"""Обработчик команды /summarize."""
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
		params = message.text.split()[1:]  # Получаем все параметры после команды
		num_messages = 0
		offset = 0

		try:
			if len(params) >= 1:
				num_messages = int(params[0])
				num_messages = max(MIN_TO_GPT, min(num_messages, MAX_TO_GPT))
			if len(params) >= 2:
				offset = int(params[1])

			return await process_summarize(message, num_messages, offset)

		except ValueError:
			await del_msg_delay(await message.answer(f"Некорректное число сообщений. Использую {DEF_SUM_MES}."))
			return await process_summarize(message, DEF_SUM_MES)
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

	ttl = await check_daily_limit(user.username, chat_id)
	#ttl = 9990
	if (ttl > 0) and (user.id not in ADMIN_IDS):
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

	chat_id_str = str(chat_id)[4:]  ## обрезка индекса для ссылки на чат
	chat = message.chat
	if chat.username:
		turl = f"t.me/{chat.username}/"
	else:
		turl = f"t.me/c/{chat_id_str}/"

	if new_messages and (last_sum_id != 0): 
		surl = f'Предыдущий свод <a href="{turl}{last_sum_id}">тут</a>'
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
								   disable_web_page_preview=True)  ## убираем превью
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
	#url="https://chatgpt-42.p.rapidapi.com/gpt4"
	#https://rapidapi.com/rphrp1985/api/chatgpt-42

	rtext = (f"Создай краткую сводку (не более {MAX_SUM} символов) "
			f"сообщений чата ключая в эту сводку ссылки на основные темы в формате: "
			f"<a href='{turl} id'> Тема </a>.\n"
			f"<a href='t.me/user_name'> full_name </a> упомянул в разговоре тему."
			f"в место Тема подставь краткое описание, например \"прокол\" или \"хорошая погода\""			
			f"reply_to означает ответ на сообщение с номером id и чем больше ответов, "
			f"тем желательнее выделять это.\n Вот сообщения чата:\n"
			f"{text}")

	await r.lpush('gpt_answ', json.dumps(rtext)) #[Тема: "подставить сюда краткое описание темы"]

	url="https://chatgpt-42.p.rapidapi.com/gpt4o" #gpt4 gpt4o deepseekai
	headers={
		"Content-Type": "application/json",
		"X-RapidAPI-Key": RAPIDAPI_KEY,
		"X-RapidAPI-Host": "chatgpt-42.p.rapidapi.com"
	}
	payload = {
		"messages": [
			{
				"role": "user",
				"content": rtext
			}
		],
		"web_access": False #deeppic
		#"model": "gpt-4o-mini"
	}
	
	async with aiohttp.ClientSession() as session:
		async with session.post(url, headers=headers, json=payload) as response:		
			if response.status == 200:				
				data = await response.json()
				await r.lpush('gpt_resp_h', json.dumps(dict(response.headers)))
				results = data.get("result", "Не удалось получить суммаризацию")
				await r.lpush('gpt_recv', json.dumps(results))				
				return results
			else:
				return f"Ошибка API: {response.status}"
			

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


# Слушать сообщения чата
@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}), ~F.text.startswith('/')) # Игнорируем команды
async def save_group_message(message: Message, bot: Bot):
	logging.info(f"Processing message in chat {message.chat.id}")  # Добавляем логирование
	user = message.from_user
	chat = message.chat
	chat_nm = chat.title
	user_name = user.username or "БезыНя-шка"
	full_name = user.full_name or "Unknown User"
	key = f"chat:{message.chat.id}:history"
	message_data = {}
	mtext=''

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
			
# save in db
	try:
		async with r.pipeline() as pipe:
			await pipe.lpush(key, json.dumps(message_data))
			await pipe.ltrim(key, 0, MAX_HISTORY - 1)
			await pipe.execute()
	
		logging.info(f"Message {message.message_id} saved successfully in {chat_nm}")
	except Exception as e:
		logging.error(f"Error saving message: {e}", exc_info=True)


# Запуск бота
async def main():
	await init_redis()
	await dp.start_polling(bot)

if __name__ == "__main__":
	asyncio.run(main())
