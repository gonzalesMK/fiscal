select sum(value) as Total,  category as Categoria from transactions as a
where 
	a.date >= "2023-04-01" and a.date <= "2023-04-31"
	and a.entry_type == "saida"
	and Categoria != "ignorar"
GROUP BY Categoria
ORDER BY  Total DESC