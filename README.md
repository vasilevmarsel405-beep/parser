# Парсер постов сотрудников (VK + Instagram → Google Sheets)

Скрипт собирает **тексты постов** и **ссылки** по заданным аккаунтам и добавляет новые записи в Google Sheets.  
Запуск планируется 2 раза в сутки через Планировщик задач Windows.

## 1) Установка (один раз, в системный Python)

```powershell
cd C:\Users\Марсель\parser
python -m pip install -r requirements.txt
```

Виртуальное окружение не используется: зависимости ставятся в тот `python`, который в PATH (или через `py -3 -m pip ...`).

## 2) Подготовка Google Sheets

1. В Google Cloud создайте Service Account.
2. Скачайте JSON-ключ и положите в проект как `service_account.json`.
3. Создайте Google-таблицу и поделитесь ей на email сервисного аккаунта (Editor).
4. Создайте лист `Posts` и заголовки в строке 1:

- `fetched_at`
- `person_name`
- `platform`
- `account`
- `published_at`
- `text`
- `url`
- `post_id`

## 3) Конфигурация

1. Скопируйте `.env.example` в `.env` и заполните значения.
2. Скопируйте `accounts.example.yaml` в `accounts.yaml` и заполните список сотрудников.

Пример:

```yaml
people:
  - person_name: "Иван Петров"
    telegram: ["durov"]
    vk: ["durov"]
    instagram: ["instagram"]
```

## 4) Токены и доступы

### Telegram (через бота)
- Нужен только `TELEGRAM_BOT_TOKEN` (получить у `@BotFather`).
- Добавьте бота в нужные каналы/чаты.
- Для каналов: бот должен быть администратором.
- В `accounts.yaml` в поле `telegram` укажите `username` канала/чата без `@` (или `chat_id`).
- Бот читает только те чаты/каналы, куда он добавлен.

### VK
- Нужен `VK_ACCESS_TOKEN` с доступом к `wall`.
- Укажите `VK_API_VERSION` (по умолчанию `5.199`).

### Instagram
- В `.env` укажите `INSTAGRAM_SESSION_ID` — значение куки `sessionid` из браузера (пока вы залогинены на instagram.com). Сессия периодически протухает — обновите куку при ошибках 401.

## 5) Проверка вручную

```powershell
cd C:\Users\Марсель\parser
python main.py --config accounts.yaml --dry-run
```

Если всё корректно:

```powershell
python main.py --config accounts.yaml
```

Пример полной выгрузки (до 200 постов на сотрудника на каждую сеть):

```powershell
python main.py --config accounts.yaml --full-history --per-platform-max 200
```

## 6) Автозапуск 2 раза в сутки (Windows Task Scheduler)

В репозитории уже есть `run_parser.bat` (переходит в папку скрипта и вызывает `python`). Для планировщика укажите путь к нему, например `C:\Users\Марсель\parser\run_parser.bat`.

Зарегистрируйте задачу:

```powershell
schtasks /Create /TN "EmployeesSocialParser" /SC DAILY /MO 1 /ST 09:00 /TR "C:\Users\Марсель\parser\run_parser.bat" /F
schtasks /Create /TN "EmployeesSocialParserEvening" /SC DAILY /MO 1 /ST 21:00 /TR "C:\Users\Марсель\parser\run_parser.bat" /F
```

## 7) Как исключаются дубли

- Локально ведется база `state.db`.
- Если пост (platform + account + post_id) уже был добавлен, повторно в таблицу он не пишется.

## 8) Важные замечания

- Учитывайте правила платформ и согласие сотрудников на мониторинг.
- Для закрытых аккаунтов может потребоваться авторизация/дополнительные права.
- Если хотите, можно расширить проект: логирование, алерты в Telegram, Docker, деплой на VPS/сервер.
