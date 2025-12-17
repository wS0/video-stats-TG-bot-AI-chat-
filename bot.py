import logging
import asyncio
from aiogram import Bot, Dispatcher, types
from openai import OpenAI
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Настройте логи
logging.basicConfig(level=logging.INFO)

# Токен Telegram-бота
BOT_TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'  # Замените на свой

# Open AI API key (используем chatgpt)
API_KEY = 'YOUR_OPEN_AI_API_KEY'

# Подключение к БД (замените на свой user:password@host/db_name)
engine = create_engine('postgresql://video_admin:video_admin@localhost/video_stats')
Session = sessionmaker(bind=engine)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()  # No 'bot' here


def nl_to_sql(user_query: str) -> str:
    schema_description = """
    База данных PostgreSQL с таблицами:

    videos (
        id text PRIMARY KEY,  -- идентификатор видео
        creator_id text,  -- идентификатор креатора
        video_created_at timestamp,  -- дата публикации видео
        views_count integer,  -- итоговые просмотры
        likes_count integer,  -- итоговые лайки
        reports_count integer,  -- итоговые жалобы
        comments_count integer,  -- итоговые комментарии
        created_at timestamp,
        updated_at timestamp
    )

    video_snapshots (
        id text PRIMARY KEY,  -- идентификатор снапшота
        video_id text REFERENCES videos(id),  -- ссылка на видео
        views_count integer,  -- текущие просмотры на момент снапшота
        likes_count integer,
        reports_count integer,
        comments_count integer,
        delta_views_count integer,  -- прирост просмотров с прошлого снапшота
        delta_likes_count integer,
        delta_reports_count integer,
        delta_comments_count integer,
        created_at timestamp,  -- время снапшота (почасово)
        updated_at timestamp
    )

    Запросы на русском языке. Даты могут быть в формате '28 ноября 2025' — преобразуй в '2025-11-28'.
    Используй TO_DATE для дат, например TO_DATE('28 ноября 2025', 'DD Month YYYY') но учти русский язык (ноября -> november, etc., или используй strftime).
    Для интервалов используй пары неравенств, обращая внимание на включение или невключение границ.
    Не используй инструкцию BETWEEN для интервалов.
    Возвращай ТОЛЬКО SQL-запрос, который возвращает ОДНО число (используй COUNT, SUM, etc.).
    Не добавляй объяснения, форматирование или ';'.
    Примеры:
    - 'Сколько всего видео есть в системе?' -> SELECT COUNT(*) FROM videos
    - 'Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?' -> SELECT COUNT(*) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND video_created_at BETWEEN '2025-11-01' AND '2025-11-05 23:59:59'
    - 'Сколько видео набрало больше 100000 просмотров за всё время?' -> SELECT COUNT(*) FROM videos WHERE views_count > 100000
    - 'На сколько просмотров в сумме выросли все видео 28 ноября 2025?' -> SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '2025-11-28'
    - 'Сколько разных видео получали новые просмотры 27 ноября 2025?' -> SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_views_count > 0 AND created_at::date = '2025-11-27'
    """

    prompt = f"{schema_description}\n\nПользовательский запрос: {user_query}\n\nSQL:"

    client = OpenAI(api_key=API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o-mini-2024-07-18",
        messages=[
            {"role": "system", "content": "Ты генерируешь SQL-запросы на основе схемы."},
            {"role": "user", "content": prompt}
        ]
    )

    sql = response.choices[0].message.content.strip()
    return sql


@dp.message()
async def handle_query(message: types.Message):
    user_query = message.text
    try:
        sql = nl_to_sql(user_query)
        session = Session()
        result = session.execute(text(sql)).scalar()
        await message.reply(str(result) if result is not None else "0")
    except Exception as e:
        logging.error(f"Error: {e}")
        await message.reply("Не удалось обработать запрос. Попробуйте перефразировать.")


async def main():
    await dp.start_polling(bot, skip_updates=True)


if __name__ == '__main__':
    asyncio.run(main())
