--user stage can store any file type html, mp4 etc
list @~;

list @%orders;

show stages;

--used in SnowSQL
--PUT file://C:/Users/User/Desktop/Sample_data/data2.csv @~/pg_userstage/;

list @~ pattern ='.*pg.*';

--remove file from stage
remove @~/pg_userstage/;


create database cricket;
create schema land;
create schema raw;
create schema clean;
create schema consumption;

-- json file format
create or replace file format my_json_format
 type = json
 null_if = ('\\n', 'null', '')  --if value is '\\n', 'null' or '' it will considered as NULL
    strip_outer_array = true
    comment = 'Json File Format with outer stip array flag true'; 


create or replace stage my_stg; 

-- lets list the internal stage
list @my_stg;

--use snowsql to upload files
--PUT file://C:/Users/User/Desktop/Snowflake/Project_ICC/2023-world-cup-json/*.json @my_stg/cricket/json/ parallel=5;

-- check if data is being loaded or not
list @my_stg/cricket/json/;

-- quick check if data is coming correctly or not
select 
        t.$1:meta::variant as meta, 
        t.$1:info::variant as info, 
        t.$1:innings::array as innings, 
        metadata$filename as file_name,
        metadata$file_row_number int,
        metadata$file_content_key text,
        metadata$file_last_modified stg_modified_ts
     from  @my_stg/cricket/json/1384433.json.gz (file_format => 'my_json_format') t;




---Raw Layer Schema SQL Scripts


'''OBJECT Data Type: Suitable for scenarios where the JSON data has a known or well-defined structure. It provides a balance between flexibility and enforceable schema.

VARIANT Data Type: Ideal for situations where the structure of the JSON data is highly dynamic and may change frequently. It provides maximum flexibility but sacrifices some level of schema enforcement'''.



-- lets create a table inside the raw layer
create or replace transient table cricket.raw.match_raw_tbl (
    meta object not null,
    info variant not null,
    innings ARRAY not null,
    stg_file_name text not null,
    stg_file_row_number int not null,
    stg_file_hashkey text not null,
    stg_modified_ts timestamp not null
)
comment = 'This is raw table to store all the json data file with root elements extracted'
;

-- we have total 33 JSON files.
copy into cricket.raw.match_raw_tbl from 
    (
    select 
        t.$1:meta::object as meta, 
        t.$1:info::variant as info, 
        t.$1:innings::array as innings, 
        --
        metadata$filename,
        metadata$file_row_number,
        metadata$file_content_key,
        metadata$file_last_modified
    from @cricket.land.my_stg/cricket/json (file_format => 'cricket.land.my_json_format') t
    )
    on_error = continue;

-- lets execute the count
select count(*) from cricket.raw.match_raw_tbl; --33

-- lets run top 10 records.
select * from cricket.raw.match_raw_tbl limit 10;





----
--Clean Layer Schema SQL Scripts
--Match Clean Table


-- step-1 how to query object col
select
meta ['data_version']::text as data_version,
meta ['created']::date as created,
meta ['revision']::number as revision
from
cricket.raw.match_raw_tbl;


-- 2 how to query variant col
select
info:match_type_number::int as match_type_number,
info:match_type::text as match_type,
info:season::text as season,
info:team_type::text as team_type,
info:overs::text as overs,
info:city::text as city,
info:venue::text as venue
from
cricket.raw.match_raw_tbl;


create or replace transient table cricket.clean.match_detail_clean as
select
    info:match_type_number::int as match_type_number, 
    info:event.name::text as event_name,
    case
    when 
        info:event.match_number::text is not null then info:event.match_number::text
    when 
        info:event.stage::text is not null then info:event.stage::text
    else
        'NA'
    end as match_stage,   
    info:dates[0]::date as event_date,
    date_part('year',info:dates[0]::date) as event_year,
    date_part('month',info:dates[0]::date) as event_month,
    date_part('day',info:dates[0]::date) as event_day,
    info:match_type::text as match_type,
    info:season::text as season,
    info:team_type::text as team_type,
    info:overs::text as overs,
    info:city::text as city,
    info:venue::text as venue, 
    info:gender::text as gender,
    info:teams[0]::text as first_team,
    info:teams[1]::text as second_team,
    case 
        when info:outcome.winner is not null then 'Result Declared'
        when info:outcome.result = 'tie' then 'Tie'
        when info:outcome.result = 'no result' then 'No Result'
        else info:outcome.result
    end as match_result,
    case 
        when info:outcome.winner is not null then info:outcome.winner
        else 'NA'
    end as winner,   

    info:toss.winner::text as toss_winner,
    initcap(info:toss.decision::text) as toss_decision,
    --
    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
    from 
    cricket.raw.match_raw_tbl;



----
select * from cricket.raw.match_raw_tbl;


--version 1  picked column
select
raw.info:match_type_number::int as match_type_number,
raw.info:players,
raw.info:teams
from cricket.raw.match_raw_tbl raw;


-- version2| taken 1 row for simplicity
select
raw.info:match_type_number::int as match_type_number,
raw.info:players,
raw.info:teams
from cricket.raw.match_raw_tbl raw
where match_type_number = 4667;

--version 3 flatterning players{} and picking key |or value
select
raw.info:match_type_number::int as match_type_number,
p.key::text as country                --- key is country name and value is player list
from cricket.raw.match_raw_tbl raw,
Lateral flatten (input => raw.info:players) p
where match_type_number = 4667;


--version 3.1 need for flatterning key {}
select
raw.info:match_type_number::int as match_type_number,
p.key::text as country,                --- key is country name and value is player list
p.value v
from cricket.raw.match_raw_tbl raw,
Lateral flatten (input => raw.info:players) p
where match_type_number = 4667;


--version 4
select
raw.info:match_type_number::int as match_type_number,
p.key::text as country,
--team. *
team.value:: text as player_name
from cricket. raw.match_raw_tbl raw,
lateral flatten (input => raw.info:players) p,
Lateral flatten (input => p.value) team
where match_type_number = 4667;




-- Player Clean Table 

create or replace table player_clean_tbl as 
select 
    rcm.info:match_type_number::int as match_type_number, 
    p.path::text as country,
    team.value:: text as player_name,
    stg_file_name ,
    stg_file_row_number,
    stg_file_hashkey,
    stg_modified_ts
from cricket.raw.match_raw_tbl rcm,
lateral flatten (input => rcm.info:players) p,
lateral flatten (input => p.value) team;


-- Lets desc table
desc table cricket.clean.player_clean_tbl;


-- add not null constraint and foreign key relationships

alter table cricket.clean.player_clean_tbl
modify column match_type_number set not null;
alter table cricket.clean.player_clean_tbl
modify column country set not null;
alter table cricket.clean.player_clean_tbl
modify column player_name set not null;

--add primary key constraint before adding that col as fk in other table (learning)
alter table cricket.clean.match_detail_clean
add constraint pk_match_type_number primary key (match_type_number);

--add foreign keys
alter table cricket.clean.player_clean_tbl
add constraint fk_match_id
foreign key (match_type_number)
references cricket.clean.match_detail_clean (match_type_number);



----- Innings array 


--version1 lets extract the elements from the innings array
select
m.info:match_type_number::int as match_type_number,
m.innings
from cricket.raw.match_raw_tbl m
where match_type_number = 4667;


--v2 
select
m.info:match_type_number::int as match_type_number,
i.value:team::text as team_name       -- root -innings , key = 0 / 1 , value = team/overs/powerplay
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i
where match_type_number = 4667;


--v3
select
m.info:match_type_number::int as match_type_number,
i.value:team::text as team_name,
o.value:over:int over,
d.value:bowler::text as bowler,
d.value:batter::text as batter,
d.value:non_striker::text as non_striker,
d.value:runs.batter::text as runs,
d.value:runs.extras::text as extras,
d.value:runs.total::text as total
from cricket.raw.match_raw_tbl m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d
where match_type_number = 4667;


--v4 
select
m.info:match_type_number::int as match_type_number,
i.value:team::text as team_name,
o.value:over::int+1 as over,
d.value:bowler::text as bowler,
d.value:batter::text as batter,
d.value:non_striker::text as non_striker,
d.value:runs.batter::text as runs,
d.value:runs.extras::text as extras,
d.value:runs.total::text as total,
e.key::text as extra_type,
e.value::number as extra_runs
from cricket.raw.match_raw_tbl m,
Lateral flatten (input => m.innings) i,
Lateral flatten (input => i.value:overs) o,
Lateral flatten (input => o.value:deliveries) d,
Lateral flatten (input => d.value:extras, outer => True) e
where match_type_number = 4667;



--v5 
select
m.info:match_type_number::int as match_type_number,
i.value:team::text as team_name,
o.value:over::int+1 as over,
d.value:bowler::text as bowler,
d.value:batter::text as batter,
d.value:non_striker::text as non_striker,
d.value:runs.batter::text as runs,
d.value:runs.extras::text as extras,
d.value:runs.total::text as total,
e.key::text as extra_type,
e.value::number as extra_runs,
w.value:player_out::text as player_out,
w.value:kind::text as player_out_kind,
w.value:fielders::variant as player_out_fielders
from cricket.raw.match_raw_tbl m,
Lateral flatten (input => m.innings) i,
Lateral flatten (input => i.value:overs) o,
Lateral flatten (input => o.value:deliveries) d,
Lateral flatten (input => d.value:extras, outer => True) e,
Lateral flatten (input => d.value:wickets, outer => True) w
where match_type_number = 4667;


--v6 (Delivery Clean Table) --transient
create or replace table cricket.clean.delivery_clean_tbl as
select
m.info:match_type_number::int as match_type_number,
i.value:team::text as team_name,
o.value:over::int+1 as over,
d.value:bowler::text as bowler,
d.value:batter::text as batter,
d.value:non_striker::text as non_striker,
d.value:runs.batter::text as runs,
d.value:runs.extras::text as extras,
d.value:runs.total::text as total,
e.key::text as extra_type,
e.value::number as extra_runs,
w.value:player_out::text as player_out,
w.value:kind::text as player_out_kind,
w.value:fielders::variant as player_out_fielders
from cricket.raw.match_raw_tbl m,
Lateral flatten (input => m.innings) i,
Lateral flatten (input => i.value:overs) o,
Lateral flatten (input => o.value:deliveries) d,
Lateral flatten (input => d.value:extras, outer => True) e,  --imp as wicket is not in every over
Lateral flatten (input => d.value:wickets, outer => True) w;

select get_ddl('table', 'cricket.clean.delivery_clean_tbl');

select distinct match_type_number from delivery_clean_tbl; --33


--add not null and fk relationships
alter table cricket.clean.delivery_clean_tbl
modify column match_type_number set not null;

alter table cricket.clean.delivery_clean_tbl
modify column team_name set not null;

alter table cricket.clean.delivery_clean_tbl
modify column over set not null;

alter table cricket.clean.delivery_clean_tbl
modify column bowler set not null;

alter table cricket.clean.delivery_clean_tbl
modify column batter set not null;

alter table cricket.clean.delivery_clean_tbl
modify column non_striker set not null;

--fk relationship
alter table cricket.clean.delivery_clean_tbl
add constraint fk_delivery_match_id
foreign key (match_type_number)
references cricket.clean.match_detail_clean (match_type_number);






--- Analysis

select * from cricket.clean.match_detail_clean
where match_type_number = 4667;

-- By batsman
select
team_name,
batter,
sum(runs) Score
from
delivery_clean_tbl
where match_type_number = 4667
group by team_name, batter
order by 1,2,3 desc;


--by team
select
team_name,
sum(runs) + sum(extra_runs) Total_score
from
delivery_clean_tbl
where match_type_number = 4667
group by team_name;



---Creating Fact & Dimension Table SQL Scripts
use database cricket;
use schema consumption;

create or replace table date_dim (
    date_id int primary key autoincrement,
    full_dt date,
    day int,
    month int,
    year int,
    quarter int,
    dayofweek int,
    dayofmonth int,
    dayofyear int,
    dayofweekname varchar(3), -- to store day names (e.g., "Mon")
    isweekend boolean -- to indicate if it's a weekend (True/False Sat/Sun both falls under weekend)
);

--drop table player_dim;

create or replace table referee_dim (
    referee_id int primary key autoincrement,
    referee_name text not null,
    referee_type text not null
);

--
create or replace table team_dim (
    team_id int primary key autoincrement,
    team_name text not null
);


-- player..
create or replace table player_dim (
    player_id int primary key autoincrement,
    team_id int not null,
    player_name text not null
);

alter table cricket.consumption.player_dim
add constraint fk_team_player_id
foreign key (team_id)
references cricket.consumption.team_dim (team_id);



--

create or replace table venue_dim (
    venue_id int primary key autoincrement,
    venue_name text not null,
    city text not null,
    state text,
    country text,
    continent text,
    end_Names text,
    capacity number,
    pitch text,
    flood_light boolean,
    established_dt date,
    playing_area text,
    other_sports text,
    curator text,
    lattitude number(10,6),
    longitude number(10,6)
);


--

create or replace table match_type_dim (
    match_type_id int primary key autoincrement,
    match_type text not null
);



--- Match fact

CREATE or replace TABLE match_fact (
    match_id INT PRIMARY KEY,
    date_id INT NOT NULL,
    referee_id INT NOT NULL,
    team_a_id INT NOT NULL,
    team_b_id INT NOT NULL,
    match_type_id INT NOT NULL,
    venue_id INT NOT NULL,
    total_overs number(3),
    balls_per_over number(1),

    overs_played_by_team_a number(2),
    bowls_played_by_team_a number(3),
    extra_bowls_played_by_team_a number(3),
    extra_runs_scored_by_team_a number(3),
    fours_by_team_a number(3),
    sixes_by_team_a number(3),
    total_score_by_team_a number(3),
    wicket_lost_by_team_a number(2),

    overs_played_by_team_b number(2),
    bowls_played_by_team_b number(3),
    extra_bowls_played_by_team_b number(3),
    extra_runs_scored_by_team_b number(3),
    fours_by_team_b number(3),
    sixes_by_team_b number(3),
    total_score_by_team_b number(3),
    wicket_lost_by_team_b number(2),

    toss_winner_team_id int not null, 
    toss_decision text not null, 
    match_result text not null, 
    winner_team_id int not null,

    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES date_dim (date_id),
    CONSTRAINT fk_referee FOREIGN KEY (referee_id) REFERENCES referee_dim (referee_id),
    CONSTRAINT fk_team1 FOREIGN KEY (team_a_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_team2 FOREIGN KEY (team_b_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_match_type FOREIGN KEY (match_type_id) REFERENCES match_type_dim (match_type_id),
    CONSTRAINT fk_venue FOREIGN KEY (venue_id) REFERENCES venue_dim (venue_id),

    CONSTRAINT fk_toss_winner_team FOREIGN KEY (toss_winner_team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_winner_team FOREIGN KEY (winner_team_id) REFERENCES team_dim (team_id)
);

