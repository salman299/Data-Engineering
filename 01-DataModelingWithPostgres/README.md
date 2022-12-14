# Data Modeling with Postgres

Hello! this repository provides a solution to Project 1 (**Project: Data Modeling with Postgres)** of the Data Engineering nano degree at the [Udacity](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## **Project Instructions**

### **Create database and tables**

- Run `python create_tables.py` to create the database and tables.
- Optional: Run `test.ipynb` Juppyter notebook to confirm the creation of the tables with the correct columns.

### **ETL Process**

After creating the database and tables we can start the ETL process:

- Run `python etl.py` This will extract data from all the .json files located in the `data` folder and store it on the Postgres database.

## Dataset

### ****Song Dataset****

The first dataset is a subset of real data from the **[Million Song Dataset](http://millionsongdataset.com/).** Each file is in JSON format and contains metadata about a song and the artist of that song. Each JSON file contains information about artists and songs.

```
data/song_data/A/B/C/TRABCEI128F424C983.json
data/song_data/A/A/B/TRAABJL12903CDCF1A.json
```

Example:

```json
{
   "num_songs":1,
   "artist_id":"ARJIE2Y1187B994AB7",
   "artist_latitude":null,
   "artist_longitude":null,
   "artist_location":"",
   "artist_name":"Line Renaud",
   "song_id":"SOUPIRU12A6D4FA1E1",
   "title":"Der Kleine Dompfaff",
   "duration":152.92036,
   "year":0
}
```

### ****Song Dataset****

The second dataset consists of log files in JSON format generated by this **[event simulator](https://github.com/Interana/eventsim)**
 based on the songs in the dataset above.

```
data/log_data/2018/11/2018-11-12-events.json
data/log_data/2018/11/2018-11-13-events.json
```

Example:

```json
{
   "artist":null,
   "auth":"Logged In",
   "firstName":"Walter",
   "gender":"M",
   "itemInSession":0,
   "lastName":"Frye",
   "length":null,
   "level":"free",
   "location":"San Francisco-Oakland-Hayward, CA",
   "method":"GET",
   "page":"Home",
   "registration":1540919166796.0,
   "sessionId":38,
   "song":null,
   "status":200,
   "ts":1541105830796,
   "userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"",
   "userId":"39"
}{
   "artist":null,
   "auth":"Logged In",
   "firstName":"Kaylee",
   "gender":"F",
   "itemInSession":0,
   "lastName":"Summers",
   "length":null,
   "level":"free",
   "location":"Phoenix-Mesa-Scottsdale, AZ",
   "method":"GET",
   "page":"Home",
   "registration":1540344794796.0,
   "sessionId":139,
   "song":null,
   "status":200,
   "ts":1541106106796,
   "userAgent":"\"Mozilla\/5.0 (Windows NT 6.1; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/35.0.1916.153 Safari\/537.36\"",
   "userId":"8"
}
```

## Database **Schema**

### **Table Artists**

```
artists (
	artist_id varchar PRIMARY KEY,
	name varchar NOT NULL,
	location varchar NOT NULL,
	latitude numeric,
	longitude numeric
);
```

### **Table Songs**

```
song (
	song_id varchar PRIMARY KEY,
	title varchar NOT NULL,
	artist_id varchar NOT NULL,
	year int NOT NULL,
	duration numeric NOT NULL
);
```

### **Table Time**

```
time (
	start_time timestamp PRIMARY KEY,
	hour int NOT NULL,
	day int NOT NULL,
	week int NOT NULL,
	month int NOT NULL,
	year int NOT NULL,
	weekday int NOT NULL
);	

```

### **Table Users**

```
users (
	user_id varchar PRIMARY KEY,
	first_name varchar NOT NULL,
	last_name varchar NOT NULL,
	gender char(1) NOT NULL,
	level varchar NOT NULL
);
```

### **Table Songplays**

```
songplays (
	songplay_id SERIAL PRIMARY KEY,
	start_time timestamp NOT NULL,
	user_id varchar NOT NULL,
	level varchar NOT NULL,
	song_id varchar,
	artist_id varchar,
	session_id int NOT NULL,
	location varchar NOT NULL,
	user_agent varchar NOT NULL
);
```

## **Representation of Star Schema**

![Untitled](static/star_schema.png)

### **Why this structure?**

You might be wondering why we have used this structure of the database? The answer to this Star Schema. Using `songplays` table we can find analytics of songs and artists. For example, It’s now easy to query the most listened song and also the most listened artist. Moreover, most of the analytical queries are doable from just one table `songplay` , Further, to find in-depth information we can JOIN other tables to get info from there. 

##
