WITH top_5_airports as (
      SELECT ORIGIN, count(ORIGIN) as count
      FROM `airline-delay-canc.airlines_data.delay_canc_data`
      Group by 1
      having count > 100000
      order by 2 desc
      limit 5
      ),
    delay_bifurcation as (
      select ORIGIN,
          (case when ARR_DELAY > 1440 then 5
             when ARR_DELAY > 300 then 4
             when ARR_DELAY > 240 then 3
             when ARR_DELAY > 30 then 2
        else 1 end) as slot

  from `airline-delay-canc.airlines_data.delay_canc_data`
  where ARR_DELAY is not null and ARR_DELAY > 0
--   and EXTRACT(year FROM FL_DATE) = 2018
  ),

  airport_timeslots as(
  select db.ORIGIN, db.slot, count(db.slot) as count
  from delay_bifurcation db,top_5_airports top5
  where top5.ORIGIN = db.ORIGIN
  group by 1,2),

  airport_struct as(
      select origin, struct(slot,count) as slot_cnt from  airport_timeslots
  ),
  udf_result as (select origin, delay_bifurcation(ARRAY_AGG(slot_cnt)) as slot_struct
  from airport_struct
  group by 1
  )
  select origin, slot_struct.cnt_1_30 as cnt_1_30min,
      slot_struct.cnt_30_2 as cnt_30min_2hr,
      slot_struct.cnt_2_5 as cnt_2_5hr,
      slot_struct.cnt_5_24 as cnt_5hr_1d,
      slot_struct.cnt_24 as cnt_1d_more
  from udf_result
