# МОБОД 2022:  _Рекомендательные Системы_. Домашнее задание.

## Данные

Датасет: 1 млн. сессий, сбор занял 2 дня.

Модель обучалась на парах данных пользователь: трек.
Отбор данных _explicit_ --> _implicit_ основывается на выражении 
```bash
IF TIME > 0.8 THEN 1 ELSE 0
````

Пример анализа данных можно посмотреть в ноутбуке `collect_data.ipynb` в папке "homework".

## Рекоммендер

- Автоэнкодер `DAE`
- Обучение на матрице `interactions`
- TOP __50__ песен для каждого пользователя
- Рекомендации выдаются порядке убывания релевантности
- Треки повторно не предлагаются, ведется учет рекоммендаций(`база Redis`)
- Вспомогательно используется TOP 100 лист реммендаций

Эксперимент проводился с раздичными значениями гиперпараметров, данные обучения отсортированны.  
Код обучения модели можно посмотреть в ноутбуке `autoencoder_dae.ipynb`.

## A/B эксперимент

Сравниваем наш рекоммендер с `CONTEXTUAL`  
Данные `20000` сессий

Логи сессий хранятся на hdfs: `/user/mob202273/my_remote_dir/`.

![A\B results](ab_res.png "results")

Пример проведения A\B эсперимента представлен в ноутбуке `result_analyzer.ipynb`.

## How To

1. Заходим на сервер по ssh, прокидываем порт  
```bash
ssh -L 16006:127.0.0.1:30007 <user>@mipt-client.atp-fivt.org
```
(порт выбираем из таблицы на wiki)
2. Создаем директорию для работы  
```bash
mkdir <dir_name>
```
3. Копируем ноутбуки для работы с данными. обучения модели и анализа результатов  
```bash
scp <from> <user>@mipt-client.atp-fivt.org:<dir>
```
4. После выполнения ноутбука со сбором информации для обучения модели получаем файл `train_data.pkl`.
5. Обучаем модель используя полученный файл. (Установка виртуального окружения описана в `README` к модулю botify  
6. Файл `recommendations.json` копируем на свою машину  
```bash
scp <user>@mipt-client.atp-fivt.org:<file_path> ../botify/data/recommendations.json
```  

7. Запускаем Docker из директории botify.
```bash
docker-compose up -d --build
```
8. Запускаем процесс симуляции
```bash
python sim/run.py --episodes 20000 --recommender remote --config config/env.yml --seed 31337
```
9. Закидываем логи контейнера в hdfs (папка script должна быть в $PYTHONPATH).
Программа dataclient.py отправляет данные в папку юзера:
```bash
python script/dataclient.py --user <user> log2hdfs --cleanup my_remote_dir
```
10. Для просмотра результатов эксперимента используем ноутбук `result_analyzer.ipynb`.