# mipt_distencode (рабочее название)

Распределение кодирования видео на пул машин

На вход принимает совместимые с MLT Framework xml-проекты

Кодирует по пресетам с помощью `melt`

Параллельная обработка одного проекта по частям пока не планируется

#### Sequence diagram

```
+-----------------+-----------------+----------------+
|     Клиент      |   Координатор   |     Воркер     |
+-----------------+-----------------+----------------+
|                 |                 |                |
| [Путь до     ]  |                 |                |
| [ mlt-проекта] -->                |                |
|                 |    [MeltJob]   -->               |
|                 |                 |   Валидация    |
|                 |                <-- ["Принято"]   |
|                <--  ["Принято"]   |                |
|                 |                 |                |
|                 |                 | Бинарник melt  |
|                 |                 | \              |
|                 |                 |  -> result.mp4 |
|                 |                 |  -> log.txt    |
|                 |                 |                |
|                 |                <--  ["Готово"]   |
|                 | "Так и запишем" |                |
+-----------------+-----------------+----------------+
```

#### Текущее состояние

По убыванию приоритета/возрастанию даты реализации

- Работает cli для клиента
- Работает учёт состояния задач на стороне координатора
- Работает полный цикл от клиента до готового mp4
- Работает (и обязательна) взаимная аутентификация
- TODO логи melt от воркера
- TODO улучшать обработку ошибок
- TODO тестировать на нескольких машинах
- TODO веб-интерфейс к координатору
- TODO аллокация воркера под ручной монтаж
- TODO смотреть состояние воркеров через веб
- TODO аутентификация клиентов (Google / stfpmi)
- Опционально, низкоуровневые задачи через ffmpeg

Ожидаемые сроки: август-сентябрь 2021

#### Набор технологий

- Язык: python 3.9
- Внутренние коммуникации: protobuf, gRPC, встроенная TLS-аутентификация
- Обработка видео: MLT Framework (`.mlt` XML schema, `melt`, `libavcodec`)
- Состояние координатора: sqlite БД через SQLAlchemy

#### Платформы

Тестировалось в пределах локалхоста на openSUSE Leap 15.2 с Python 3.9
