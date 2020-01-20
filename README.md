# Comepos Fetcher

Cet outil permet de récupérer aussi aisément que possible les données COMEPOS provenant
de l'API Vesta System.

La librairie est en deux parties : le sous-module `io` bas niveau permettant la
connection à l'API web (via `uplink` et `request`), et le sous-module `database` un peu
plus haut niveau offrant une interface utilisateur plus soignée. Les données retournées
sont systématiquement convertis en DataFrame pandas pour donner à l'utilisateur toute
les possibilité venant avec ces structures de données (indexation, filtration, export
des données, aggregat et interpolation...).

## Installation

l'installation se fait via pip et git (ce dernier outil peut s'installer via conda avec
`conda install git`):

```bash
pip install git+https://github.com/locie/comepos_fetcher
```

## Utilisation

```python
from comepos_fetcher.database import ComeposDB

>>> login = "my_email_adress@my_provider.com"
>>> mdp = "***********"

>>> comepos_db = ComeposDB(username=login, password=mdp)
ComeposDB(username='my_email_adress@my_provider.com', store_location=Path('...'))
>>> comepos_db.buildings
... building info ...
# We get the first building id
>>> building_id = comepos_db.buildings.index[0]
>>> my_building_db = comepos_db.get_building_db(building_id)

# Or, if the building id is already known
from comepos_fetcher.database import BuildingDB
>>> my_building_db = BuildingDB(username=login, password=mdp, building_id="that_building_id")
```

Une fois que la base de donnée est initialisé, les données sont stocké en cache pour
éviter de devoir les récupérer à chaque fois depuis la source.

```python

>>> my_building_db.sensors_info
... sensor info ...

>>> sensor = my_building_db.sensors.service_n_variable_m
>>> sensor
Sensor(zone='...', device='...', service_name='service_n', variable_name='variable_m')

>>> sensor.data # first time, if there is a lot of data to fetch
service_n_variable_m:   9%|▉         | 3/32 [00:03<00:24]
... data values ...
# the second time, data will be available without waiting-time.
# last sensor values can be fetch with
>>> sensor.refresh()
# wich will only retrieve data that have been added since the previous fetch?
```

Tous les capteurs peuvent être récupérés (et / ou mis à jour) d'un coup avec `my_building_db.refresh_all_sensors()`. C'est une opération qui sera longue (1h pour 100+ capteurs, dépend des données et de la connection).

Les `sensor.data` étant des dataframe pandas, il est possible de les utiliser pour tracer des graphes, faire des indexation sur la date, de les aggréger...

```python

>>> sensor.data["2018"] # get only 2018 data
>>> sensor.data["2018": "07/03/2019"]] # 1st Jan. 2018 to 7th Mar. 2019
>>> sensor.data["2018"].resample("1W").mean() # Weekly averaged data for 2018
>>> sensor.data["2018"].resample("1W").mean().interpolate().plot() # as prev. with interpolated missing value, use maplotlib to do the plot
# and so on
```
