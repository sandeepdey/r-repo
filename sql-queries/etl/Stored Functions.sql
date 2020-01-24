CREATE FUNCTION iif_(BOOLEAN, float, float) RETURNS float 
stable 
as $$ 
	SELECT CASE $1 WHEN TRUE THEN $2 ELSE $3 END
$$ language sql;