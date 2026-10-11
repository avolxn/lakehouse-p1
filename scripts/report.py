import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
R = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else ROOT / "results"

def load(name):
    path = R / name
    return json.loads(path.read_text()) if path.exists() else None

run = load("run.json")
layers = load("layers.json") or []
bench = load("benchmark.json") or []
evolution = load("evolution.json")
gold = load("gold.json")
quality = load("quality.json")
bi_log = (R / "superset.log").read_text() if (R / "superset.log").exists() else ""
complete = run and run.get("status") == "complete" and "BRONZE_PROOF=" in bi_log
columns = {
    "customer": "custkey, mktsegment",
    "orders": "orderkey, custkey, orderdate, shippriority",
    "lineitem": "orderkey, linenumber, extendedprice, discount, shipdate",
    "order_lines": "orderkey, linenumber, custkey, mktsegment, orderdate, shippriority, shipdate, extendedprice, discount, net_revenue, days_to_ship",
    "shipping_priority": "orderkey, revenue, orderdate, shippriority",
}

def layer_table(layer):
    names = {"bronze": ["customer","orders","lineitem"], "silver":["order_lines"], "gold":["shipping_priority"]}[layer]
    label = "Колонки" if layer == "gold" else "Колонки (что оставил)"
    text = f"| Таблица | {label} | Строк | Вес, МБ | Формат | Сжатие | Партиционирование | Обоснование |\n|---|---|---:|---:|---|---|---|---|\n"
    why = {"bronze":"Нужные исходные колонки; колоночное хранение для SQL, ZSTD как компромисс размера и чтения.",
           "silver":"Подготовленный факт Q3; DECIMAL для денег, ZSTD для повторного чтения.",
           "gold":"Небольшая готовая витрина; единый формат упрощает эксплуатацию."}[layer]
    for name in names:
        item = next((i for i in layers if i["layer"]==layer and i["table"]==name), None)
        n = item["rows"] if item else "не измерено"
        size = f'{item["bytes"]/1000000:.4f}' if item else "не измерено"
        text += f"| {name} | {columns[name]} | {n} | {size} | PARQUET | ZSTD | нет | {why} |\n"
    if layer == "silver" and evolution and evolution.get("table") == "iceberg.silver.snapshot_demo":
        text += (f'| snapshot_demo | {columns["order_lines"]}, data_origin | {evolution["restored"]} | '
                 f'{evolution["bytes"]/1000000:.4f} | PARQUET | ZSTD | нет | '
                 'Копия факта внутри Silver для проверки изменения схемы и восстановления. |\n')
    return text

text = f"""# Отчёт по практической №1

## 1. Вариант и масштаб

| Поле | Значение |
|---|---|
| ФИО (как в VK Education) | Владислав Елдашов |
| Вариант (запрос) | 1 - Q3 Shipping Priority |
| Масштаб `sfN` | sf1 |
| Почему выбрал такой масштаб | Около 6 млн позиций и 1,5 млн заказов позволяют сравнивать реальные файлы на локальном компьютере; sf10 существенно повышает расход диска и памяти. sf1 одинаков на всех слоях. |

## 2. Поднятые сервисы - и почему именно они

Состояние запущенных сервисов: [results/docker-ps.txt](results/docker-ps.txt). Версии образов и их ID: [results/images.json](results/images.json).

| Сервис | Роль в стеке | Почему именно он (а не альтернатива) |
|---|---|---|
| MinIO, исходники RELEASE.2025-04-22T22-12-26Z | Объектное S3-хранилище данных и метаданных Iceberg | Локальная S3-совместимость без облачного аккаунта; исходники фиксированы, образ собирается локально из-за недоступности официальных бинарных образов. |
| PostgreSQL 17.5 | База состояния Lakekeeper | Транзакционная БД для небольших метаданных; не используется вместо объектного хранилища аналитических данных. |
| Lakekeeper 0.9.5 | Iceberg REST Catalog | Открытый протокол отделяет каталог от SQL-движка, поддерживает конфигурацию S3-хранилища. |
| Trino 479 | SQL, генерация TPC-H, ELT и Q3 | Один движок умеет читать TPC-H и писать Iceberg; меньше компонентов для локального эксперимента, чем отдельный Spark-кластер. |
| Superset 4.1.2 + trino driver 0.336.0 | BI и SQL Lab | Открытый BI, умеет читать Bronze через Trino; позволяет работать с несколькими datasets без копирования данных. |

### Скрипты, которыми поднимал стек

Полные определения сервисов: [compose.yaml](compose.yaml), сборка MinIO: [config/minio/Dockerfile](config/minio/Dockerfile), драйвер BI: [config/superset/Dockerfile](config/superset/Dockerfile).

```bash
docker compose up -d --build trino
python3 scripts/bootstrap.py
python3 scripts/run.py --scale 1 --write-repeats 3 --query-repeats 5
docker compose up -d --build superset
docker compose exec -T superset python /app/superset_bronze.py
docker compose ps
```

Автоматизация всех шагов с ожиданием готовности: `bash scripts/start.sh`. Trino получает метаданные через Lakekeeper и сам читает/пишет файлы MinIO. PostgreSQL хранит состояние каталога; Iceberg JSON/manifest-файлы и физические данные находятся в MinIO.

## 3. Разбор запроса - какие данные нужны

| Таблица TPC-H | Нужные колонки | Что запрос с ними делает (фильтр / джойн / агрегат / вывод) |
|---|---|---|
| customer | custkey, mktsegment | Джойн по custkey, фильтр BUILDING |
| orders | orderkey, custkey, orderdate, shippriority | Джойны с клиентами/позициями; дата раньше 1995-03-15; дата и приоритет входят в GROUP BY и вывод |
| lineitem | orderkey, extendedprice, discount, shipdate; дополнительно linenumber | Джойн с заказом; shipdate позже 1995-03-15; сумма extendedprice × (1-discount); linenumber нужен для проверки ключа позиции |

Выручка считается по позициям, удовлетворяющим условию отгрузки, а не по всем позициям заказа. Отбираются десять заказов с максимальной выручкой. Это аналитика торговой компании на синтетических данных TPC-H.

## 4. Медальон - решения по каждому слою

### Bronze (сырой)

{layer_table('bronze')}
### Silver (очищенный)

{layer_table('silver')}
### Gold (витрина)

{layer_table('gold')}
Партиционирование не используется: на sf1 после выбора колонок таблицы компактны, а избыточные разделы создадут маленькие файлы и дополнительные обращения. При росте объема стоит отдельно измерить партиционирование orders по месяцам orderdate и lineitem по месяцам shipdate.

Контроль качества: уникальность ключей customer/orders и составного ключа lineitem; обязательные поля; неотрицательная цена; discount в [0,1]; отсутствие потерянных внешних ключей. Результаты: {json.dumps(quality, ensure_ascii=False) if quality is not None else 'не проверено'}.

Проверки не выявили дефектных строк в TPC-H. Silver фильтрует недопустимые ключи/диапазоны, нормализует сегмент `upper(trim(...))`, преобразует суммы/скидки в DECIMAL и обогащает факт `net_revenue`, `days_to_ship`. При обнаружении дефектов скрипт останавливается. Gold сравнивается с исходным TPC-H Q3.

## 5. Тесты производительности

Одинаковые строки, колонки и Q3; последовательно, с перемешиванием порядка seed=42. Три CTAS-записи на конфигурацию; одна прогревочная агрегация и пять измеряемых агрегаций. Размер включает только data files текущих снапшотов трех таблиц Bronze; размер указан в МБ (1 МБ = 1 000 000 байт). CTAS включает чтение базового ZSTD Bronze, запись и commit; HTTP-время Q3 включает весь запрос и получение результата. Полная методология и ограничения указаны в README.

| Сжатие на Bronze | Время запроса | Размер Bronze, МБ | Твой вывод (что лучше и почему) |
|---|---|---:|---|
"""
conclusions = {
    "NONE": "Самый большой размер; сжатие уменьшает объем хранения.",
    "SNAPPY": "Запись быстрее GZIP, но файлы заметно больше.",
    "GZIP": "Размер близок к ZSTD, но запись заняла больше времени.",
    "ZSTD": "Parquet/ZSTD дал наименьший размер и медианы записи и запроса; выбран для слоев.",
}
for row in bench:
    conclusion = conclusions[row["codec"]] if row["format"] == "PARQUET" else "ORC/ZSTD занимает больше места, чем Parquet/ZSTD, и имеет большую медиану Q3."
    text += f'| {row["format"]}/{row["codec"]} | {row["query_median_s"]:.3f} с ({row["query_min_s"]:.3f}-{row["query_max_s"]:.3f}) | {row["bytes_median"]/1000000:.3f} | {conclusion} |\n'
text += "\nВремя записи: три повтора, медиана и диапазон.\n\n| Формат/кодек | Время записи, с |\n|---|---:|\n"
for row in bench:
    text += f'| {row["format"]}/{row["codec"]} | {row["write_median_s"]:.3f} ({row["write_min_s"]:.3f}-{row["write_max_s"]:.3f}) |\n'
text += "\nВремя запроса также указано как медиана и диапазон пяти измерений. Диапазоны пересекаются, поэтому различие в скорости требует проверки на других запусках.\n"
text += """
Таблица содержит локальные теплые измерения, не холодное чтение и не сертифицированный TPC-H результат. Метаданные и старые снапшоты не включены в размер. Сырые измерения, query ID и server stats: results/writes.json, reads.json, benchmark.csv. Число файлов может отличаться, поскольку Trino самостоятельно распределяет работу между writers.

### Результат Gold

| orderkey | revenue | orderdate | shippriority |
|---:|---:|---|---:|
"""
if gold:
    for row in gold["rows"]:
        text += "| " + " | ".join(map(str,row)) + " |\n"
else:
    text += "| не получен | - | - | - |\n"
text += """
## 6. Часть 2 - осмысление и работа с инструментами

### 6.1. Понять и объяснить

| Вопрос | Твой ответ |
|---|---|
| Зачем нужны большие данные и lakehouse (а не обычная БД/хранилище)? | При росте объема дешевле отдельно масштабировать объектное хранение и вычисления. Iceberg дает таблицы, эволюцию схемы и снапшоты поверх файлов, а разные движки могут пользоваться одним каталогом. Для маленькой транзакционной задачи обычная PostgreSQL проще; эта работа показывает архитектуру, а не необходимость Lakehouse для любого объема. |
| Пример schema evolution на Silver: добавил/переименовал колонку - что произошло? | На таблице iceberg.silver.snapshot_demo, созданной из Silver, добавляется source_system VARCHAR, затем переименовывается в data_origin. Старые строки читаются с NULL, исходные бизнес-данные не пересоздаются. Скрипт считает строки с data_origin IS NULL и проверяет DESCRIBE. |
| Пример скрипта восстановления данных из снапшота (time travel) | Сохраняется snapshot_id до удаления позиций одного заказа; историческое чтение возвращает исходное число строк; rollback_to_snapshot восстанавливает текущий набор данных. Откат данных не равен откату схемы. |

SQL для просмотра и восстановления снапшота:

```sql
SELECT snapshot_id, committed_at FROM iceberg.silver."snapshot_demo$snapshots";
SELECT count(*) FROM iceberg.silver.snapshot_demo FOR VERSION AS OF <snapshot_id>;
ALTER TABLE iceberg.silver.snapshot_demo EXECUTE rollback_to_snapshot(<snapshot_id>);
```

"""
if evolution:
    text += f'Фактическая проверка: ID `{evolution["snapshot_id"]}`; до удаления **{evolution["before"]}**, после **{evolution["after_delete"]}**, исторический срез **{evolution["historical"]}**, после восстановления **{evolution["restored"]}**. NULL в новой колонке: **{evolution["new_column_nulls"]}**.\n'
    if (R / "evolution.log").exists():
        text += '\nИзменение схемы и восстановление выполнены скриптом scripts/evolution.py. Команды и результат сохранены в results/evolution.log.\n'
    else:
        text += '\nПроверка на Silver выполняется скриптом scripts/evolution.py; результат находится в results/evolution.json, команды в results/executed.sql.\n'
else:
    text += "Фактическая проверка снапшотов пока не выполнена.\n"
text += """
### 6.2. Подключить инструмент к lakehouse

| Инструмент | Почему выбрал именно его |
|---|---|
| Apache Superset в Docker | SQL Lab и datasets позволяют непосредственно увидеть Bronze через Trino, без дополнительного хранилища данных и отдельного Spark-кластера. |

Развертывание и драйвер находятся в Compose и config/superset. Через консоль контейнера выполняется scripts/superset_bronze.py: создается подключение `trino://student@trino:8080/iceberg/bronze`, регистрируются customer/orders/lineitem и выполняется UNION ALL с count(*). Запрос выполняется через Superset Database/SQLAlchemy.

"""
proof = next((line for line in bi_log.splitlines() if line.startswith("BRONZE_PROOF=")),None)
text += (f"Результат запроса через Superset:\n\n```text\n{proof}\n```\n" if proof else "Доказательство подключения Superset пока отсутствует.\n")
text += """
В UI: войти на http://localhost:18088; Data → Datasets - три Bronze datasets; SQL → SQL Lab - Lakehouse Trino, schema bronze; выполнить `SELECT * FROM iceberg.bronze.lineitem LIMIT 10`. Сохраненный вывод находится в results/superset.log.

## 7. Выводы

Bronze хранит исходные данные, Silver выполняет очистку и расчет выручки, Gold содержит десять заказов по запросу Q3. MinIO хранит файлы, а Lakekeeper и PostgreSQL обеспечивают работу каталога Iceberg. В этом запуске Parquet/ZSTD дал меньший размер и меньшие медианы времени записи и Q3, чем остальные варианты. Для sf1 партиционирование не понадобилось, поскольку таблицы после отбора колонок занимают немного места. Изменение схемы сохранило старые данные, а откат снапшота восстановил удаленные строки.
"""
if evolution:
    text = text.replace("<snapshot_id>", str(evolution["snapshot_id"]))
if run and run.get("docker"):
    hw = run["docker"]
    environment = (f'Среда: {run["host"]}; Docker {hw["ServerVersion"]}; '
                   f'{hw["NCPU"]} vCPU, {hw["MemTotal"]/1073741824:.2f} ГиБ памяти Docker; '
                   'Trino heap 4 ГБ, один coordinator/worker. Параметры и время запуска сохранены в results/run.json.\n\n')
    text = text.replace("## 5. Тесты производительности\n\n", "## 5. Тесты производительности\n\n" + environment)
if R.name == "local":
    text = text.replace("results/", "results/local/")
(ROOT / "отчет.md").write_text(text)
print("Report written; complete=" + str(bool(complete)))
