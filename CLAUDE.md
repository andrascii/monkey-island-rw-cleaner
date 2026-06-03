# Monkey Island VPN

Monkey Island VPN - это коммерческий VPN.

## Workspace Layout

These paths describe my local workspace.
When a task may affect another service, inspect the relevant service code before changing anything.

- Telegram Bot: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-vpn-bot`
- Website: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-website`
- User Notify: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-user-notify`
- RWMS: `/Users/apugachev/work/repos/my/vpn/monkey-island/rwms`
- YM-STAT: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-ym-stat`
- RW Cleaner: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-rw-cleaner`
- Payment: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-payment`
- Email Service: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-email`
- Custom Config: `/Users/apugachev/work/repos/my/vpn/monkey-island/monkey-island-custom-config`

## Cross-Service Awareness

This is not a single-service project.

Before changing code, check whether the change affects:

- database schema
- protobuf/gRPC contracts
- payment flow
- subscription lifecycle
- Remnawave synchronization
- Telegram bot UX
- website cabinet behavior
- notification logic

If another service depends on the changed behavior, inspect that service too and either update it or explicitly explain why no update is needed.

# Главная точка входа пользователя это телеграм бот.

## Алгоритм работы телеграм бота следующий:
1. Пользователь заходит в бот и нажимает кнопку start.
2. Если это новый пользователь, для него генерируется подписка и бесплатный пробный период на 7 дней путем создания записи в таблице users в postgres и создания одноименной подписки в remnawave панели через rwms сервис.
3. Далее бот позволяет установить подписку пользователю на устройство и дает возможность создать инвойс на оплату подписки, а также присылает уведомления о заканчивающемся времени подписки, успешные оплаты и прочее.

Также есть вебсайт, который служит для этих же целей.

При изменении кода обязательно всегда поправляй локальный README.md того проекта, который исправляешь.
Если исправляешь структуру таблиц базы данных, самостоятельно миграцию для alembic не создавай, для этого есть отдельный скрипт.

# Архитектурные правила

## Общие правила (самое важное)
- Изменения в одном из микросервисов часто влекут за собой потребности в изменении других микросервисов, поэтому самое главное и важное правило, после любых изменений в любом из перечисленных микросервисов проекта Monkey Island (tg bot, user-notify, payment, website, rwms, ym-stat, rw-cleaner, payment, email, custom-config) в обязательном порядке надо проверить корректность работы всех остальных микросервисом. Будут ли новые правки работать согласованно со всеми остальными микросервисами, если же нет, то надо вносить правки и в соседние микросервисы для обеспечения работоспособности всего проекта в целом.
- Критически важно всегда проверять, что правки не приводят к случайному удалению данных из таблицы users (со всеми связанными с ней таблицами) и панели remnawave. Панель remnawave, которая хранит подписки пользоватейлей и управляется через RWMS сервис хранит самую важную и критически значимую часть бизнеса, недопустимо случано удалить оттуда какую-то подписку, если непредусмотрено иное (сервис rw-cleaner).
- Всегда пиши тесты, покрывающие новую часть кода и если не хватает тестов на старую часть кода, обязательно дописывай их.

После изменений оцени, какие микросервисы могут быть затронуты.

Если изменяются:

- protobuf
- grpc API
- модели БД
- контракты сообщений
- бизнес-логика подписок

обязательно перечисли потенциально затронутые сервисы.

## База данных (common)
- Всегда используй SQLAlchemy ORM.
- Не используй raw SQL до тех пор пока явно не потребуется их наличие.
- Все изменения схемы базы данных должны происходить строго через Alembic migrations.
- Никогда не модифицируй production таблицы без миграции.
- Не модифицируй таблицы без понимания имеющейся бизнес логики.

Запрещено:

- удалять записи
- изменять семантику полей
- менять ограничения

без понимания существующей бизнес-логики.

## Project Facts
- Database: PostgreSQL
- ORM: SQLAlchemy 2.x
- Bot: aiogram 3.x
- VPN Panel: Remnawave
- VPN Protocols:
  - VLESS Reality
  - XHTTP Reality
  - Hysteria2

## Telegram Bot
- При каждом изменении поправляй README.md файл.
- При изменении сервисных админских команд держи команду /help согласованной с реализацией.

## Вебсайт
- VPN домены это домены где показывается посадочные страницы с прямой подачей как VPN продукт
- VPS (или neutral domains) это домены, где никакогоу поминания VPN нет, позиционируются они как VPS виртуальные серверы, завуалированно дающее понять пользователю, что на самом деле продается VPN, эт осделано для возможности рекламироваться, т.к. напрямую рекламировать VPN в РФ нельзя
- Cabinet domains это домены, где на главной странице показывается форма авторизации в личный кабинет
- Личный кабинет позволяет управлять пользовательской подпиской, продлевать ее оплачивая, устанавливать локально на свое устройство.
- Все домены обязательно должны хорошо отображаться на мобильных устройствах, вся верстка, т.к. мобильные устройства это основные устройства, с которых лиды приходят на сайт
- Сайт должен взаимодействовать с основной базой данных сервиса также через sqlalchemy
- На всех доменах кроме cabinet domains при переходе на страницу авторизации однократно должен показывать онбординг из 5 слайдов, перечисляющий самые важные преимущества сервиса

## User-notify
- Применяй правила из пункта "Общие правила (самое важное)"

## RWMS
- Применяй правила из пункта "Общие правила (самое важное)"

## YM-STAT
- Применяй правила из пункта "Общие правила (самое важное)"

## RW-CLEANER
- Применяй правила из пункта "Общие правила (самое важное)"

## PAYMENT
- Применяй правила из пункта "Общие правила (самое важное)"

## Email сервис
- Применяй правила из пункта "Общие правила (самое важное)"

## Custom-config
- Применяй правила из пункта "Общие правила (самое важное)"

## Перед каждым рефактором

Ты обязан:

1. Прочитать существующие реализации.
2. Объяснить почему это изменение необходимо.
3. Поддерживать обратную совместимость.
4. Избегать масштабных переработок текста, если это не было явно запрошено.

## Business Context

ВАЖНО:

- Пользователи платят реальные деньги.
- Поломка подписок считается критическим инцидентом.
- Поломка рекуррентных платежей считается критическим инцидентом.
- При сомнениях выбирать наиболее безопасное решение.

## Источники истины

Источниками истины являются подписки в Remnawave панели и база данных sqlalchemy.

## gRPC Compatibility

Backward compatibility is mandatory.

Never:

- remove fields from protobuf messages
- change field numbers
- rename protobuf fields

without explicit migration plan.

## Logging

Log:

- payments
- subscription creation
- subscription renewal
- referral bonuses
- Remnawave API calls
- grpc failures

Never log:

- passwords
- access tokens
- private keys
- payment secrets

## Before Implementing Any Change

You must first:

1. Explain current implementation.
2. Explain proposed implementation.
3. Explain risks.
4. List affected services.
5. Only then write code.

## When modifying a project:

update README.md if:

- new environment variables appear
- new API endpoints appear
- startup procedure changes
- database migrations are required
- architecture changes

## Production First

This is a production system.

Prefer:

- stability
- compatibility
- predictability

over:

- code elegance
- architectural purity
- large refactoring

## Remnawave Safety Rules

Remnawave stores active VPN subscriptions and is one of the most critical parts of the business.

By default, never:

- delete subscriptions
- recreate subscriptions
- regenerate user UUIDs
- regenerate subscription URLs

unless the task explicitly requires it.

Existing active client configurations must remain functional.

### Allowed exception: RW Cleaner

The `monkey-island-rw-cleaner` service is allowed to delete expired Remnawave subscriptions only when all of the following conditions are true:

1. The subscription is expired.
2. The expiration date is older than the configured retention period.
3. The user has no active paid subscription.
4. The deletion candidate was selected by the cleaner logic, not manually guessed.
5. The operation is logged.
6. The operation is safe to retry.
7. A dry-run mode exists or is preserved.
8. The service never deletes active, trial-active, paid-active, or recently expired subscriptions.

When modifying `monkey-island-rw-cleaner`, always preserve safety checks that prevent accidental deletion of active subscriptions.

## RW Cleaner Rules

RW Cleaner is the only service that may delete expired subscriptions from Remnawave.

It must be conservative.

Required safety features:

- configurable retention period
- dry-run mode
- structured logs for every deletion candidate
- structured logs for every actual deletion
- idempotent behavior
- no deletion of active users
- no deletion of recently expired users
- no deletion when database state is ambiguous
- no deletion when Remnawave state is ambiguous

If there is any mismatch between PostgreSQL and Remnawave, do not delete automatically.
Log the mismatch and skip the subscription.