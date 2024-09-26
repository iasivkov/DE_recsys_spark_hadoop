# Витрина на основе Pyspark, HDFS, parquet

### Описание
Полный пайплайн для создания витрины данных для системы рекомендаций кандидатов в друзья в социальной сети на основе местоположения и активности пользователя.

### Структура репозитория
Внутри `src` расположены две папки:
- `/src/dags` — папка с Airflow дагами реализующими выполнение пайплайна 
- `/src/scripts` — папка со скриптами, содержащими вспомогательные инструменты и  логику реализации этапов пайплайна.

### Описание дагов
- first_dag_sprint7_city_dag

### Порядок запуска дагов
- сначала запускаем first_dag_sprint7_city_dag
- потом остальные два в любом порядке

### Структура HDFS хранилища
- В папке /user/iasivkov/data/geo лежит parquet-файл events, содержит информацию о событиях с координатами
(stage слой, обновляется с некоторой периодичностью) 
- В папке /user/iasivkov/snapshots/geo лежит parquet-файл geo_t, таблица с координатами городов
- В папке /user/iasivkov/data/dds с помощью first_dag_sprint7_city_dag создаётся parquet-файл events_city, содержит информацию о событиях и городе
(dds-слой, обновляется раз в день)
- В папке /user/iasivkov/data/analytics лежат parquet-файлы dm_region, dm_user, dm_rec_sys  
с витринами по регионам, пользователям и рекомендациям друзей, создаются первые два с помощью sprint7_user_dag и последний — sprint7_region_dag
(первые два обновляются раз в день: в 9:00 и 12:00 соответственно и последний раз в неделю, в понедельник в 00:00)
