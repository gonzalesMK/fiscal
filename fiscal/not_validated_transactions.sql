SELECT "_rowid_",* 
FROM "main"."transactions" 
WHERE    "validated" = '0'  
		AND "entry_type" LIKE 'SAIDA' 
		AND "category" NOT LIKE 'Ignorar'
ESCAPE '\' 
ORDER BY value ASC LIMIT 49999 OFFSET 0;
