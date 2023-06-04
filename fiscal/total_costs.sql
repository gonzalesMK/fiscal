select 	sum(value) as Total
from transactions as a
where 
	a.date >= "2023-04-01" AND
	a.date <= "2023-04-31" AND
	a.category != "ignorar" AND
	entry_type == "saida" 
	--AND category == "compras"
	
ORDER BY  Total DESC