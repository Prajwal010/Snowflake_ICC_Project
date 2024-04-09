-- step-1:creating straem on raw table for 3 different stream
create or replace stream cricket.raw.for_match_stream on table cricket.raw.match_raw_tbl append_only = true;
create or replace stream cricket.raw.for_player_stream on table cricket.raw.match_raw_tbl append_only = true;
create or replace stream cricket.raw.for_delivery_stream on table cricket.raw.match_raw_tbl append_only = true;

-- step-2: Creating a task that runs every 5min to load json data into raw layer.
create or replace task cricket.raw.Load_json_to_raw
warehouse = 'COMPUTE_WH'
schedule = '5 minute'
as
copy into cricket.raw.match_raw_tbl from(
select
t.$1:meta::object as meta,
t.$1:info::variant as info,
t.$1:innings::array as innings,
metadata$filename,
metadata$file_row_number,
metadataSfile_content_key,
metadata$file_last_modified
from @cricket.land.my_stg/cricket/json (file_format > cricket.Land.my_json_format) t
)
on_error = continue;


--Step-3: Creating another child task to read stream & Load data into clean Layer
create or replace task cricket.raw.Load_to_clean_match
warehouse = "COMPUTE_WH"
after cricket.raw.Load_json_to_raw
when system$stream_has_data('cricket.raw.for_match_stream')
as
insert into cricket.clean_match_detail_clean
select
info:match_type_number::Int as match_type_number,
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
    when info:outcome.winner is not null then "Result Declared"
    when info:outcome.result = 'tie' then "Tie"
    when info:outcome.result = "no result" then "No Result"
    else info:outcome.result
end as match_result,
case
    when info:outcome.winner is not null then info:outcome.winner
else "NA"
end as winner,
info:toss.winner:itext as toss_winner,
initcap(info:toss.decision::text) as toss_decision,
--
stg_file_name,
stg_file_row_number.
stg-file_hashkey,
stg_modified_ts
from
cricket.raw.for_match_stream;



--Step-4: creating a child task after match data is populated|
create or replace task cricket.raw.Load_to_clean_player
warehouse = "COMPUTE_WH"
    after cricket.raw.Load_to_clean_match
        when system$stream_has_data('cricket.raw.for_player_stream')
    as
insert into cricket.clean.player_clean_tbl
select
rcm.info:match_type_number::int as match_type_tbl,
p.path::text as country,
team.value::text as player_name,
stg_file_name,
stg_file_row_number,
stg_file_hashkey,
stg_modified_ts
from cricket.raw.for_player_stream rcm,
Lateral flatten (input => rcm.info:players) p,
Lateral flatten (input => p.value) team;



-- step-5: creating delivery clean table
create or replace task cricket.raw.Load_to_clean_delivery
warehouse = "COMPUTE_WH"
    after cricket.raw.Load_to_clean_player
        when system$stream_has_data ('cricket.raw.for_delivery_stream')
as
insert into cricket.clean.delivery_clean_tbl
select
m.info:match_type_number::int as match_type_number,
i.value:team::text as team_name,
o.value.over::int as over,
d.value:bowler::text as bowler,
d.value:batter::text as batter,
d.value:non_striker::text as non_striker,
d.value:runs.batter::text as runs,
d.value:runs.extras::text as extras,
d.value:runs.total::text as total,
o.key::text as extra_type,
e.value::number as extra_runs,
w.value:player_out::text as player_out,
w.value:kind::text as player_out_kind,
w.value:fielders::variant as player_out_fielders,
m.stg_file_name,
m.stg_tile_row_number,
m.stg_file_hashkey,
m.stg_modified_ts
from cricket.raw.for_delivery_stream m,
Lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
Lateral flatten (input => d.value:extras, outer => True) e,
Lateral flatten (input => d.value:wickets, outer => True) w;


--step-6

create or replace task cricket.raw.load_to_team_dim
warehouse = "COMPUTE_WH"
after cricket.raw.load_to_clean_delivery
as
insert into cricket.consumption.team_dim (team_name)
select distinct team_name
from (
    select first_team as team_name from cricket.clean.match_detail_clean
    union all
    select second_team as team_name from cricket.clean.match_detail_clean
)
minus
select team_name from cricket.consumption.team_dim;



-- Step 7
create or replace task cricket.raw.load_to_player_dim
warehouse = 'COMPUTE_WH'
after cricket.raw.Load_to_clean_delivery
as
    insert into cricket. consumption. player_dim (team_id, player_name)
        select b.team_id, a.player_name
    from
        cricket. clean.player_clean_tbl a join cricket.consumption.team_dim b
        on a.country=b.team_name
        group by
        b.team_id,
        a.player_name
    minus
        select tean_id, player_name from cricket.consumption.player_dim;


-- step 8

create or replace task cricket.raw.Load_to_venue_dim
warehouse = "COMPUTE_WH"
after cricket.raw.load_to_clean_delivery
as
insert into cricket.consumption.venue_dim (venue_name, city)
select venue, city
from (
    select
        venue,
        case
            when city is null then 'NA'
            else city
        end as city
    from cricket.clean.match_detail_clean
    group by venue_name, city
)
minus
select venue_name, city from cricket.consumption.venue_dim;




--step 9


-- Step-9: populate the fact table]

-- Replace "COMPUTE_WH" with the actual name of your warehouse
create or replace task cricket.raw.Load_match_fact
warehouse = "COMPUTE_WH"
after cricket.raw.load_to_team_dim, cricket.raw.load_to_player_dim, cricket.raw.load_to_venue_dim
as
insert into cricket.consumption.match_fact
select a.* from (
    select
        m.match_type_number as match_id,
        dd.date_id as date_id,
        0 as referee_id,
        ftd.team_id as first_team_id,
        std.team_id as second_team_id,
        mtd.match_type_id as match_type_id,
        vd.venue_id as venue_id,
        50 as total_overs,
        6 as balls_per_over,
        max(case when d.team_name = m.first_team then d.over else 0 end) as overs_played_by_team_a,
        sum(case when d.team_name = m.first_team then 1 else 0 end) as balls_played_by_team_a,
        sum(case when d.team_name = m.first_team then d.extras else 0 end) as extra_balls_played_by_team_a,
        sum(case when d.team_name = m.first_team then d.extra_runs else 0 end) as extra_runs_scored_by_team_a,
        0 as fours_by_team_a,
        0 as sixes_by_team_a,
        (sum(case when d.team_name = m.first_team then d.runs else 0 end) +
         sum(case when d.team_name = m.first_team then d.extra_runs else 0 end)) as total_runs_scored_by_team_a,
        sum(case when d.team_name = m.first_team and player_out is not null then 1 else 0 end) as wicket_lost_by_team_a,
        
        max(case when d.team_name = m.second_team then d.over else 0 end) as overs_played_by_team_b,
        sum(case when d.team_name = m.second_team then 1 else 0 end) as balls_played_by_team_b,
        sum(case when d.team_name = m.second_team then d.extras else 0 end) as extra_balls_played_by_team_b,
        sum(case when d.team_name = m.second_team then d.extra_runs else 0 end) as extra_runs_scored_by_team_b,
        0 as fours_by_team_b,
        0 as sixes_by_team_b,
        (sum(case when d.team_name = m.second_team then d.runs else 0 end) +
         sum(case when d.team_name = m.second_team then d.extra_runs else 0 end)) as total_runs_scored_by_team_b,
        sum(case when d.team_name = m.second_team and player_out is not null then 1 else 0 end) as wicket_lost_by_team_b,
        
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
        join cricket.clean.delivery_clean_tbl d on d.match_type_number = m.match_type_number
        join team_dim tw on m.toss_winner = tw.team_name
        join team_dim mw on m.winner = mw.team_name
    -- where m.match_type_number = 4686
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
) a;



--step 10
-- Replace 'COMPUTE_WН' with the actual name of your warehouse
create or replace task cricket.raw.Load_delivery_fact
warehouse = "COMPUTE_WН"
after cricket.raw.load_match_fact
as
insert into cricket.consumption.delivery_fact
select a.* from (
    select
        d.match_type_number as match_id,
        td.team_id,
        bpd.player_id as bowler_id, 
        spd.player_id as batter_id, 
        nspd.player_id as non_striker_id,
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
        join player_dim nspd on d.non_striker = nspd.player_name
) a;




--resume all the task
use role accountadmin;
--GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin:
--use role sysadmin:


alter task cricket.raw.Load_delivery_fact resume;
alter task cricket.raw.Load_match_fact resume;
alter task cricket.raw.Load_to_venue_dim resume;
alter task cricket.raw.Load_to_player_dim resume;
alter task cricket.raw.Load_to_team_dim resume;
alter task cricket.raw.Load_to_clean_delivery resume;
alter task cricket.raw.Load_to_clean_player resume;
alter task cricket.raw.Load_to_clean_match resume;
alter task cricket.raw.Load_json_to_raw resume;
