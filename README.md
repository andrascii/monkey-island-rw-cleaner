# monkey-island-rw-cleaner

Safe cleanup service for old expired RWMS subscriptions.

The service never deletes rows from the SQLAlchemy `users` table. It only deletes
RWMS users after checking both RWMS and the local database.

## Safety Model

Deletion is allowed only when all checks pass:

- RWMS user has `expire_at`;
- RWMS `expire_at` is older than `GRACE_DAYS`;
- local `users` row exists by `username`, unless `--include-orphans` is explicitly used;
- local `users.expire_at` exists;
- local `users.expire_at` is in the past and older than `GRACE_DAYS`;
- user has no recent pending YooKassa payment;
- user has no non-expired WATA invoice without a paid transaction;
- delete count is below `MAX_DELETIONS`;
- the RWMS user is re-read immediately before deletion and the full eligibility
  decision is calculated again;
- destructive flags such as `--execute` and `--include-orphans` must be passed
  explicitly on the command line, not only through environment variables.

Default mode is dry-run. `EXECUTE=true` in the environment is intentionally ignored;
real deletion requires the explicit `--execute` flag.

## Downstream effects on other services

After a subscription is deleted from Remnawave, the local `users` row still exists,
so the user can appear in services without a matching RWMS subscription. This was
reviewed across the project:

- **Payment** (`rwms_tasks_processor.py`) and both referral bonus paths
  (`payment_handlers.py`, user-notify `user_traffic_progress_watcher.py`) detect a
  missing RWMS user and recreate it via `AddUser`. A new payment or referral bonus
  fully restores the subscription. No money or bonus is lost.
- **User-notify** drives expiration/expired notifications from the local
  `users.expire_at`, so a missing RWMS subscription does not break notifications.
- **Telegram bot** "Install VPN" and "My Profile" handlers detect the missing RWMS
  subscription and offer renewal (tariffs keyboard) instead of a generic error.
- **Website cabinet** shows a "contact support" message for the subscription URL
  until the user pays, which recreates the subscription.

Because only subscriptions expired longer than `GRACE_DAYS` (>= 30 days) are deleted,
affected users cannot use the VPN anyway until they renew.

## Environment

Required:

```bash
DATABASE_URL=postgresql+psycopg2://user:password@host:5432/db
RWMS_HOST=127.0.0.1
RWMS_PORT=50052
```

Optional:

```bash
GRACE_DAYS=30
PENDING_PAYMENT_GRACE_HOURS=48
PAGE_SIZE=500
MAX_DELETIONS=25
USERNAME_ALLOWLIST=
```

`GRACE_DAYS` cannot be lower than 30.

## Run

Dry-run:

```bash
python main.py
```

Real deletion:

```bash
python main.py --execute
```

Very conservative first production run:

```bash
python main.py --execute --max-deletions 3
```

Test one known username only:

```bash
python main.py --username-allowlist 4f1d... --execute --max-deletions 1
```

## Tests

```bash
pytest
```



## Стабы RWMS

`proto/rwmanager.proto` — копия из `monkey-island-rwms/proto/`; стабы
`proto/rwmanager_pb2*.py` генерируются ТОЛЬКО собственным `.venv` по пинам
`requirements-dev.txt` (`grpcio-tools==1.81.1`, `protobuf==6.33.6`):

```bash
cp ../monkey-island-rwms/proto/rwmanager.proto proto/rwmanager.proto
PATH="$PWD/.venv/bin:$PATH" ./makepb.sh
```

`rwmanager_pb2_grpc.py` несёт `GRPC_GENERATED_VERSION` и при runtime
grpcio старше сгенерированной версии роняет импорт `RuntimeError`, поэтому
стабы из website/vpn-bot (grpcio 1.81.0) сюда не копировать. С 2026-09
proto знает `TrafficLimitStrategy.MONTH_ROLLING = 4` (антиабьюз) — без
нового стаба значение из панели приходило бы как неизвестный enum.

## Зависимости

Все прямые зависимости в `requirements.txt` запинены на точные версии (инцидент 2026-07-07: незапиненный `remnawave` в rwms при пересборке Docker-образа притянул 2.8.0 с breaking change, и бот показывал всем пользователям, что срок ключа истёк). Пересборка образа не должна молча подтягивать новые версии. Обновление любой версии — осознанное изменение: поднять пин в `requirements.txt`, прогнать тесты и проверить согласованность со смежными сервисами.
