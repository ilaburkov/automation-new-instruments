# Курсовая работа. Бурков Илья ПМИ 235

Автоматизация работы с новыми биржевыми инструментами в высокочастотной торговле

В данном проекте представлены следующие файлы:

Скрипт на Python, который запрашивает все инструменты с биржи, и далее сохраняет их в файл, при следующем запуске повторяет действие. Далее смотрит на отличия, и если поменялась информация о новых инструментах то присылает сообщение в Slack. Команда запуска `python3 ./scripts/listings.py --channel "channel_name"` Так как код работает со Slack, я добавил флаг `--test`, который позволит проверяющим запустить код, без взаимодействия со Slack.

Основной код находится в папке funds_controller, и написан на с++. Код работает с СУБД ClickHouse. С помощью него можно выполнить множество команд, например взять/отдать займ, открыть/закрыть хедж, перевести средства/займы между субаккаунтами.
Код использует другие файлы, которых нет в проекте, данные файлы не относятся к проекту и не играют ключевой роли в реализации логики. 

Примеры таблиц:
```
CREATE TABLE LOANS_INFO_v2 (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64,
    subaccount LowCardinality(String),
    asset LowCardinality(String),
    amount Decimal(21, 12),
    initial_subaccount LowCardinality(String),
    type LowCardinality(String),
    loan_id String,
    status LowCardinality(String),
) ENGINE = SummingMergeTree(amount)
ORDER BY (subaccount, loan_id, asset); 

CREATE TABLE TRANSACTIONS_v2 (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime64,
    from_subaccount LowCardinality(String),
    from_wallet LowCardinality(String),
    to_subaccount LowCardinality(String),
    to_wallet LowCardinality(String),
    asset String,
    amount Decimal(21, 12),
    type LowCardinality(String),
    inner_id String,
    status LowCardinality(String),
) ENGINE = MergeTree()
ORDER BY (timestamp);
```
Базовый пример таблиц. Пример с реальными данными, есть в отчете
<img width="1203" alt="Screenshot 2025-04-01 at 23 14 18" src="https://github.com/user-attachments/assets/fa23d6cd-7632-4bd4-9644-99d7e0d71edc" />


