-- this assumes that you already have a max_rmc_action_id and max_rrp_action_id chosen
-- they should be equal to lastRmcActionId and lastRrpActionId from the stream application

drop table if exists consumers_score_start;

UPDATE config_parameters set value = substring(date_trunc('hour', now())::text, 1, 16) where key = 'AFFINITY_DECREMENT_FROM';

create table consumers_score_start as
select system_id, consumer_id, brand_id, sum(score) as score from (
	select ca.system_id, ca.consumer_id,
	case when s.brand_id in (117, 125, 127, 138) then s.brand_id
		 when substring(ca.payload_json->>'sku_bought',1,2) = 'WI' then 127
		 when substring(ca.payload_json->>'sku_bought',1,2) = 'SB' or substring(ca.payload_json->>'sku_bought',1,2) = 'SO' then 125
		 when substring(ca.payload_json->>'sku_bought',1,2) = 'LG' then 138
		 when substring(ca.payload_json->>'sku_bought',1,2) = 'CA' then 117
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
	where acts.is_complex = false and ca.external_system_date > now()-'2 year'::interval and s.brand_id in (117, 125, 127, 138, 486)
		and ( (ca.id <=:lastRmcActionId and ca.system_id = 1) or (ca.id <=:lastRrpActionId and ca.system_id = 2))
) as foo
group by system_id, consumer_id, brand_id;


--insert into consumers_score_start (system_id, consumer_id, brand_id, score)
--select cb.id, cb.brand, 0
--from (select system_id, consumer_id, brand from consumers cross join (select 117 brand union select 125 brand union select 127 brand union select 138 brand) brands) cb
--left join consumers_score_start cs on cb.system_id = cs.system_id and cb.consumer_id = cs.id_consumer and cb.brand = cs.brand_id
--where cs.system_id is null and cs.id_consumer is null;

alter table consumers_score_start add column id serial;