# Postgres DB documentation

## Setup

```bash
docker run --rm -it --name postgres-setup -v /data/jupyterhub/postgres/crtm/:/var/lib/postgresql/data -e POSTGRES_PASSWORD=[password] postgres:12
```

Once this is done, we can stop the container. The basic structure and cnfiguration
is ready, we won't need to pass the `POSTGRES_PASSWORD` environment variable again.

## Running it

```bash
docker run -d --restart always --name postgres-crtm --user 1000:1000 -v /data/jupyterhub/postgres/crtm/:/var/lib/postgresql/data -v /data/jupyterhub/user_data/sgn/crtm_inter_exploration/:/data/:ro --network jupyterhub postgres:12
```

## Usage

### Accesing the CLI

```bash
docker exec -it postgres-crtm psql -U postgres
```

## Table creation

### crtm_poll

#### Create the table

```pgsql
CREATE TABLE crtm_poll
(actual_date timestamp with time zone, cod_stop text,
cod_line text, cod_issue text,
eta timestamp with time zone, destination_stop text);
```

#### Read the table data from the CSV file

```pgsql
COPY crtm_poll FROM
'/data/crtm_poll_data'
WITH (FORMAT csv, HEADER);
```

*Note: took 3 minutes to read 95M rows*

### arrival_times

#### Create the table

```pgsql
CREATE TABLE arrival_times
(cod_issue text, cod_stop text, cod_line text,
eta_date smallint, arrival_time timestamp with time zone);
```

#### Read the table data from the CSV file

```pgsql
COPY arrival_times FROM
'/data/paralel_arrival_times_consecutive.csv'
WITH (FORMAT csv, HEADER);
```
