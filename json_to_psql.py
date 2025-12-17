import json
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Подключение к PostgreSQL (user, password, host, db)
engine = create_engine('postgresql://video_admin:video_admin@localhost/video_stats')

Base = declarative_base()


class Video(Base):
    __tablename__ = 'videos'
    id = Column(String, primary_key=True)
    creator_id = Column(String)
    video_created_at = Column(DateTime(timezone=False))
    views_count = Column(Integer)
    likes_count = Column(Integer)
    reports_count = Column(Integer)
    comments_count = Column(Integer)
    created_at = Column(DateTime(timezone=False))
    updated_at = Column(DateTime(timezone=False))


class VideoSnapshot(Base):
    __tablename__ = 'video_snapshots'
    id = Column(String, primary_key=True)
    video_id = Column(String, ForeignKey('videos.id'))
    views_count = Column(Integer)
    likes_count = Column(Integer)
    reports_count = Column(Integer)
    comments_count = Column(Integer)
    delta_views_count = Column(Integer)
    delta_likes_count = Column(Integer)
    delta_reports_count = Column(Integer)
    delta_comments_count = Column(Integer)
    created_at = Column(DateTime(timezone=False))
    updated_at = Column(DateTime(timezone=False))


# Создаем таблицы, если не существуют
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Загружаем JSON
with open('videos.json', 'r') as f:
    data = json.load(f)

for v in data['videos']:
    # Преобразуем строки в datetime
    v['video_created_at'] = datetime.fromisoformat(v['video_created_at'].replace('Z', '+00:00')).replace(tzinfo=None)
    v['created_at'] = datetime.fromisoformat(v['created_at'].replace('Z', '+00:00')).replace(tzinfo=None)
    v['updated_at'] = datetime.fromisoformat(v['updated_at'].replace('Z', '+00:00')).replace(tzinfo=None)

    video = Video(**{k: v[k] for k in v if k != 'snapshots'})
    session.add(video)

    for s in v['snapshots']:
        s['created_at'] = datetime.fromisoformat(s['created_at'].replace('Z', '+00:00')).replace(tzinfo=None)
        s['updated_at'] = datetime.fromisoformat(s['updated_at'].replace('Z', '+00:00')).replace(tzinfo=None)
        snapshot = VideoSnapshot(**s)
        session.add(snapshot)

session.commit()
print("Данные загружены.")
