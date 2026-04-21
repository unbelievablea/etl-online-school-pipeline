# ETL-пайплайн для онлайн-школы

Учебный проект по построению пайплайна обработки данных с использованием Airflow и миграций базы данных.

## Задача

Организовать регулярную выгрузку, трансформацию и загрузку данных об активности студентов онлайн-школы в витрину данных.

## Технологии

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)

## Что сделано

- Спроектирован DAG в Apache Airflow с задачами на извлечение, трансформацию и загрузку (ETL)
- Написаны SQL-миграции для обновления схемы базы данных
- Настроено Docker-окружение для локального запуска (PostgreSQL, Airflow, VSCode)
- Оркестрация зависимостей между задачами в DAG

## Результат

- Пайплайн автоматически обновляет витрину данных каждый час
- Обрабатываются инкрементальные обновления (только новые данные)
- Миграции позволяют версионировать изменения в БД

## Как запустить

```bash
# Запуск контейнера с окружением
docker run -d --rm -p 3000:3000 -p 15432:5432 --name=de-project-sprint-3-server cr.yandex/crp1r8pht0n0gl25aug1/project-sprint-3:latest

# После запуска доступны:
# - Airflow: http://localhost:3000
# - PostgreSQL: localhost:15432
