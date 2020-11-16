-- this assumes that you already have a max_rmc_action_id and max_rrp_action_id chosen
-- they should be equal to lastRmcActionId and lastRrpActionId from the stream application

begin;

drop table if exists consumers_score_start;

UPDATE config_parameters set value = date_trunc('milliseconds', now()-'2 year'::interval)::text where key = 'AFFINITY_DECREMENT_FROM';

create table consumers_score_start as
select system_id, consumer_id, brand_id, sum(score) as score from (
	select ca.system_id, ca.consumer_id,
	case when s.brand_id in (117, 125, 127, 138) then s.brand_id
	     when s.brand_id = 13 and ca.system_id = 1 then 138
		 when substring(ca.payload_json->'value'->>'sku_bought',1,2) = 'WI' then 127
         when substring(ca.payload_json->'value'->>'sku_bought',1,2) = 'SB' or substring(ca.payload_json->'value'->>'sku_bought',1,2) = 'SO' then 125
         when substring(ca.payload_json->'value'->>'sku_bought',1,2) = 'LG' then 138
         when substring(ca.payload_json->'value'->>'sku_bought',1,2) = 'CA' then 117
		 else -1
	end as brand_id,
	case when acts.is_complex = false then acts.score
		 when acts.is_complex = true then
		 	case when ca.system_id = 1 and ca.action_id = 1964 then 2+10*coalesce((ca.payload_json->'value'->>'sku_quantity')::int, 0)
		 		 when ca.system_id = 1 and ca.action_id = 1740 then 2+10*coalesce((ca.payload_json->'value'->>'sku_quantity')::int, 0)
		 		 when ca.system_id = 1 and ca.action_id = 2080 then
		 		 	case when ca.payload_json->'value'->>'new_value' = 'true' and
		 		 			  ca.payload_json->'value'->>'old_value' = 'false' then 1 else 0 end
		 		 when ca.system_id = 2 and ca.action_id = 2080 then
		 		 	case when ca.payload_json->'value'->>'new_value' = 'true' and
		 		 			  ca.payload_json->'value'->>'old_value' = 'false' then 1 else 0 end
		 		 when ca.system_id = 2 and ca.action_id = 3064 then affinity.get_score_from_logic_order(ca.payload_json->'value'->'Products'->'Product')
		 		 when ca.system_id = 2 and ca.action_id = 1898 then
		 		 	case when upper(ca.payload_json->'value'->>'prize_name') like '%DEVICE%CRYSTALE%' then 20
		 		 		 when upper(ca.payload_json->'value'->>'prize_name') like '%DEVICE%' or upper(ca.payload_json->'value'->>'prize_name') like '%CAPSULES%' then 10
		 		 		 else 12
		 		 	end
		 	end
	end as score
	from consumer_actions ca
	join affinity.action_scores acts using (action_id, system_id)
	join recoded_subcampaignes s on (ca.payload_json->>'subcampaignId')::int = s.id and ca.system_id = s.system_id
	where ca.external_system_date > now()-'2 year'::interval and s.brand_id in (13, 117, 125, 127, 138, 486)
		and ( (ca.id <=:lastRmcActionId and ca.system_id = 1) or (ca.id <=:lastRrpActionId and ca.system_id = 2))
) as foo
where brand_id > 0
group by system_id, consumer_id, brand_id;

update consumers set payload = payload-'affinity_117'-'affinity_125'-'affinity_127'-'affinity_138';

insert into consumers (system_id, consumer_id, payload, updated_at)
select system_id, consumer_id,
json_object_agg('affinity_'||brand_id,
    json_build_object('lut', round(extract(epoch from now()) * 1000)::text, 'value', score::text)
) as payload, now()
from consumers_score_start
group by system_id, consumer_id
on conflict on constraint consumers_pk do update
set payload = consumers.payload || excluded.payload, updated_at = now();

commit;
