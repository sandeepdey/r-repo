CREATE FUNCTION iif_(BOOLEAN, float, float) RETURNS float 
stable 
as $$ 
	SELECT CASE $1 WHEN TRUE THEN $2 ELSE $3 END
$$ language sql;


CREATE FUNCTION if_varchar(BOOLEAN, VARCHAR, VARCHAR) RETURNS VARCHAR 
stable 
as $$ 
	SELECT CASE $1 WHEN TRUE THEN $2 ELSE $3 END
$$ language sql;


CREATE FUNCTION scrape_filter(VARCHAR, VARCHAR, VARCHAR) RETURNS BOOLEAN 
-- $1 : geo , $2 : pharmacy , $3 : site
-- use like scrape_filter(geo,pharmacy,site)
stable 
as $$ 
	SELECT 	$1 != 'all' AND $2 NOT LIKE '%all%' AND $2 != 'other_pharmacies' AND $3 = 'goodrx'
$$ language sql;


CREATE FUNCTION date_range_filter(DATE, DATE, DATE) RETURNS BOOLEAN 
-- $1 : date , $2 : range start date , $3 : range end date
stable 
as $$ 
	SELECT 	$1::date >= $2::date AND $1::date <= $3::date
$$ language sql;




