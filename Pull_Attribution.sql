WITH attrib AS (select * from(
select empi,
		last_value(orgn ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS orgn,
		last_value(orgtin ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS orgtin,
		last_value(pcpnpi ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS pcpnpi,
		last_value(fn ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS fn,
		last_value(ln ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt  asc rows UNBOUNDED PRECEDING) AS ln,
		last_value(mn ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS mn,
		last_value(gn ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS gn,
		last_value(pcpn ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS pcpn,
		last_value(sln ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS sln,
		last_value(prnm ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS prnm,
		last_value(plnm ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS plnm,
		last_value(rn ignore nulls) OVER (PARTITION BY empi ORDER BY atrdt asc rows UNBOUNDED PRECEDING) AS rn,
		ROW_NUMBER() over (partition by empi order by atrdt::date desc,ingdt::date desc) as rowno 
from l2.pd_attribution
where lower(atrl)='aco'
AND plid IN ('##','##','##','##','##','##')
and atrdt >= '2###-01-01'
)
where rowno=1),
id AS (SELECT DISTINCT empi AS empi_id,id 
from l2.pd_attribution pa 
where atrdt >= '2021-01-01'
and lower(atrl) = 'payer'
and plid IN ('##','##','##','##','##','##'))
SELECT * FROM attrib 
RIGHT OUTER JOIN id on empi = id.empi_id
