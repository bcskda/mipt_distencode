# mipt_distencode (рабочее название)

Распределение кодирования видео на пул машин

На вход принимает совместимые с MLT Framework xml-проекты

Кодирует по пресетам с помощью `melt`, пресеты лежат в `config/presets`

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

#### Про эксплуатацию

Порт координатора TCP 50052, порт воркера TCP 50053

Самым удобным способом развернуться выглядит: сделать на координатора и воркеров DNS-записи и скриптом репозитория самоподписанные сертификаты

grpcio поддерживает IPv4/IPv6

#### Попробовать

```bash
. venv/bin/activate
. config/manager-1.env
python -m mipt_distencode.manager.server
```

```bash
. venv/bin/activate
. config/worker-1.env
python -m mipt_distencode.worker.server
```

```bash
. venv/bin/activate
. config/manager-1.env  # todo веб
python -m mipt_distencode.manager.client manager-1.localdomain PostMeltJob "$HOME/Project 1.mlt" 1080p_nvenc_vbr "/media/share/Project 1.mp4"
```
