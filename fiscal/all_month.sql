select * from transactions as a
where 
	a.date >= "2023-02-01" and a.date <= "2023-02-31"
	and a.entry_type == "SAIDA"