#-------------------------------------------------------------------------------
# Простой тест для проверки промта и ответа от ИИ
#-------------------------------------------------------------------------------

# Возможно потребуется установить pip install requests
import requests

# Зайти на сойт https://rapidapi.com/rphrp1985/api/chatgpt-42/pricing
# и выбрать бесплатный план для тестирования
# вернуться на уровень выше и выбрать модель (GPT-4o или иную)
# скопировать свой ключ сюда

rapidapi_key = "123"

# Добавить текст из файла
def open_data():
    with open('чат.txt', 'r', encoding='utf-8') as file:
        return file.read()

def send_gpt(data):
#-------------------------------------------------
# Можно поиграть с разными моделями
    url = "https://chatgpt-42.p.rapidapi.com/gpt4"

    payload = {
        "messages": [
            {
                "role": "user",
                "content": data
            }
        ],
        "model": "gpt-4o-mini"
    }
    headers = {
        "x-rapidapi-key": rapidapi_key,
        "x-rapidapi-host": "chatgpt-42.p.rapidapi.com",
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    return (response.json())

#----------------------------------------------------
# Главная функция отправки запроса в ИИ
def main():

# Отправка текста и содержимого файла
    text = ("Привет ИИ!\n"
    "Что в этом тексте:\n") + open_data()

    print(send_gpt(text))

if __name__ == '__main__':
    main()
