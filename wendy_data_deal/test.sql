

SELECT province_id, provinceName, country_id, country, confirmedCount, suspectedCount, curedCount, deadCount, updateTime 
FROM prov_data
ORDER BY updateTime
LIMIT 1;

-- SELECT fields FROM table ORDER BY id DESC LIMIT 1;


