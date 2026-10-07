# Task Tracker Scheduler

Микросервис для автоматического формирования ежедневных отчётов по задачам пользователей.

Сервис по расписанию получает данные пользователей, запрашивает отчёт у Summarization Service и передаёт готовый результат сервису отправки email.

## Возможности

* Периодический запуск задач
* Получение пользователей
* Формирование ежедневных отчётов
* Взаимодействие с Summarization Service через Kafka RPC
* Отправка готовых отчётов в Email Sender

## Технологии

**Backend:**

* Java
* Spring Boot
* Spring Scheduler
* Maven

**Messaging:**

* Apache Kafka
* Spring Kafka

**DevOps:**

* Docker
* Docker Compose

## 🌐 API Endpoints

### Scheduler

| Method  | Endpoint         | Description                 |
|---------| ---------------- | --------------------------- |
| GET     | `/api/scheduler` | Запуск формирования отчётов |

## Клонирование репозитория

```bash
git clone https://github.com/eriicyaan/task-tracker-scheduler.git
```

## Запуск

Для запуска всего проекта используйте инфраструктурный репозиторий:

[task-tracker-infrastructure](https://github.com/eriicyaan/task-tracker-infrastructure)
