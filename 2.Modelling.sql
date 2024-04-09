

-- Data population

CREATE or replace transient TABLE cricket.consumption.date_rnage01 (Date DATE);
insert into cricket.consumption.date_rnage01 (date) values 
('2000-01-01'), ('2000-01-02'), ('2000-01-03'), ('2000-01-04'), ('2000-01-05'), ('2000-01-06'), ('2000-01-07'), ('2000-01-08'), ('2000-01-09'), ('2000-01-10'), ('2000-01-11'), ('2000-01-12'), ('2000-01-13'), ('2000-01-14'), ('2000-01-15'), ('2000-01-16'), ('2000-01-17'), ('2000-01-18'), ('2000-01-19'), ('2000-01-20'), ('2000-01-21'), ('2000-01-22'), ('2000-01-23'), ('2000-01-24'),  ('2003-07-15'), ('2003-07-16'), ('2003-07-17'), ('2003-07-18'), ('2003-07-19'), ('2003-07-20'), ('2003-07-21'), ('2003-07-22'), ('2003-07-23'), ('2003-07-24'), ('2003-07-25'), ('2003-07-26'), ('2003-07-27'), ('2003-07-28'), ('2003-07-29'), ('2003-07-30'), ('2003-07-31'), ('2003-08-01'), ('2003-08-02'), ('2003-08-03'), ('2003-08-04'), ('2003-08-05'), ('2003-08-06'), ('2003-08-07'), ('2003-08-08'), ('2003-08-09'), ('2003-08-10'), ('2003-08-11'), ('2003-08-12'), ('2003-08-13'), ('2003-08-14'), ('2003-08-15'), ('2003-08-16'), ('2003-08-17'), ('2003-08-18'), ('2003-08-19'), ('2003-08-20'), ('2003-08-21'), ('2003-08-22'), ('2003-08-23'), ('2003-08-24'), ('2003-08-25'), ('2003-08-26'), ('2003-08-27'), ('2023-12-31');


INSERT INTO cricket.consumption.date_dim (Date_ID, Full_Dt, Day, Month, Year, Quarter, DayOfWeek, DayOfMonth, DayOfYear, DayOfWeekName, IsWeekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY Date) AS DateID,
    Date AS FullDate,
    EXTRACT(DAY FROM Date) AS Day,
    EXTRACT(MONTH FROM Date) AS Month,
    EXTRACT(YEAR FROM Date) AS Year,
    CASE WHEN EXTRACT(QUARTER FROM Date) IN (1, 2, 3, 4) THEN EXTRACT(QUARTER FROM Date) END AS Quarter,
    DAYOFWEEKISO(Date) AS DayOfWeek,
    EXTRACT(DAY FROM Date) AS DayOfMonth,
    DAYOFYEAR(Date) AS DayOfYear,
    DAYNAME(Date) AS DayOfWeekName,
    CASE When DAYNAME(Date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS IsWeekend
FROM cricket.consumption.date_rnage01;


select * from cricket.consumption.date_dim;





---
--v2
insert into cricket.consumption.team_dim (team_name)
select distinct team_name from (
select first_team as team_name from cricket.clean.match_detail_clean
union all
select
second_team as team_name from cricket.clean.match_detail_clean
) order by team_name;



--v4 insert the data
insert into cricket.consumption.player_dim (team_id, player_name)
select b.team_id, a.player_name
from
cricket.clean.player_clean_tbl a join cricket.consumption.team_dim b
on a.country = b.team_name
group by
b.team_id,
a.player_name;

--v5 check the data
select * from cricket.consumption.player_dim:



-- forgot to grab refree details in clean table

select
info
from
cricket.raw.match_raw_tbl Limit 1;


'''create or replace table referee_dim (
    referee_id int primary key autoincrement,
    referee_name text not null,
    referee_type text not null
);'''


select
info:officials.match_referees[0]::text as match_referee,
info:officials.reserve_umpires[0]::text as reserve_umpire,
info:officials.tv_umpires[0]::text as tv_umpire,
info:officials.umpires[0]::text as first_umpire,
info:officials.umpires[1]::text as second_umpire
from
cricket.raw.match_raw_tbl Limit 1;



insert into cricket.consumption.venue_dim (venue_name,city)
select venue, city from (
select
venue,
case when city is null then 'NA'
else city
end as city
from cricket.Clean.match_detail_clean)
group by
venue,city;

select * from cricket.consumption.venue_dim where city = 'Pune';
select city from cricket.consumption.venue_dim group by city having count(1) >= 1;


---
-- v3
insert into cricket.consumption.match_type_dim (match_type)
select match_type from cricket.clean.match_detail_clean group by match_type;


-- Date Dimension]
select min(event_date), max(event_date) from cricket.clean.match_detail_clean;




==============

--Fact Table Data Population Script

insert into cricket.consumption.match_fact 
select 
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_A,
    sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_A,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.team_name = m.first_team then  d.runs else 0 end ) + sum(case when d.team_name = m.first_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_A,
    sum(case when d.team_name = m.first_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_a,    
    
    max(case when d.team_name = m.second_team then  d.over else 0 end ) as OVERS_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  1 else 0 end ) as balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extras else 0 end ) as extra_balls_PLAYED_BY_TEAM_B,
    sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) as extra_runs_scored_BY_TEAM_B,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.team_name = m.second_team then  d.runs else 0 end ) + sum(case when d.team_name = m.second_team then  d.extra_runs else 0 end ) ) as total_runs_scored_BY_TEAM_B,
    sum(case when d.team_name = m.second_team and player_out is not null then  1 else 0 end ) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.match_result as match_result,
    mw.team_id as winner_team_id
     
from 
    cricket.clean.match_detail_clean m
    join date_dim dd on m.event_date = dd.full_dt
    join team_dim ftd on m.first_team = ftd.team_name 
    join team_dim std on m.second_team = std.team_name 
    join match_type_dim mtd on m.match_type = mtd.match_type
    join venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket.clean.delivery_clean_tbl d  on d.match_type_number = m.match_type_number 
    join team_dim tw on m.toss_winner = tw.team_name 
    join team_dim mw on m.winner= mw.team_name 
    --where m.match_type_number = 4686
    group by
        m.match_type_number,
        date_id,
        referee_id,
        first_team_id,
        second_team_id,
        match_type_id,
        venue_id,
        total_overs,
        toss_winner_team_id,
        toss_decision,
        match_result,
        winner_team_id
        ;

--Delivery Fact Table Scripts

CREATE or replace TABLE delivery_fact (
    match_id INT ,
    team_id INT,
    bowler_id INT,
    batter_id INT,
    non_striker_id INT,
    over INT,
    runs INT,
    extra_runs INT,
    extra_type VARCHAR(255),
    player_out VARCHAR(255),
    player_out_kind VARCHAR(255),

    CONSTRAINT fk_del_match_id FOREIGN KEY (match_id) REFERENCES match_fact (match_id),
    CONSTRAINT fk_del_team FOREIGN KEY (team_id) REFERENCES team_dim (team_id),
    CONSTRAINT fk_bowler FOREIGN KEY (bowler_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_batter FOREIGN KEY (batter_id) REFERENCES player_dim (player_id),
    CONSTRAINT fk_stricker FOREIGN KEY (non_striker_id) REFERENCES player_dim (player_id)
);

-- insert record
insert into delivery_fact
select 
    d.match_type_number as match_id,
    td.team_id,
    bpd.player_id as bower_id, 
    spd.player_id batter_id, 
    nspd.player_id as non_stricker_id,
    d.over,
    d.runs,
    case when d.extra_runs is null then 0 else d.extra_runs end as extra_runs,
    case when d.extra_type is null then 'None' else d.extra_type end as extra_type,
    case when d.player_out is null then 'None' else d.player_out end as player_out,
    case when d.player_out_kind is null then 'None' else d.player_out_kind end as player_out_kind
from 
    cricket.clean.delivery_clean_tbl d
    join team_dim td on d.team_name = td.team_name
    join player_dim bpd on d.bowler = bpd.player_name
    join player_dim spd on d.batter = spd.player_name
    join player_dim nspd on d.non_striker = nspd.player_name;

-- 2000 matches * 600 balls per match = 1,200,000


