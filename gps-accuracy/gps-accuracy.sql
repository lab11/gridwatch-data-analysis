SELECT
    powerwatch.core_id,
    st,
    site_id,
    max(time) as maxtime,
    min(time) as mintime,
    count(*) as N_gps_samples,
    avg(gps_satellites) as gps_satellites_avg,
    avg(gps_latitude) AS lat_avg,
    max(lat_fixed) as lat_fixed,
    avg(gps_longitude) AS lon_avg,
    max(lon_fixed) as lon_fixed,
-- we're close enough to equator that we don't bother to multiply longitude difference by cos(radians(latitutde0) before squaring it
    110.25 * 1000 * sqrt(pow(avg(gps_latitude)-max(lat_fixed),2) +
	pow(avg(gps_longitude)-max(lon_fixed),2)) AS dist_diff_meters
FROM
    powerwatch
    INNER JOIN (
    SELECT core_id,
	location_latitude as lat_fixed,
	location_longitude as lon_fixed,
        CAST(site_id as INTEGER) as site_id,
	COALESCE(deployment_start_time, '1970-01-01 00:00:00+0') as st,
	COALESCE(deployment_end_time, '9999-01-01 00:00:00+0') as et
    FROM deployment
	) d ON powerwatch.core_id = d.core_id
WHERE
    time >= date '2018-07-01'
    AND time <= date '2019-09-01'
    AND time >= st
    AND time < et
-- I don't think this ever ends up NULL; my guess is if there's no GPS it's NaN
    AND gps_longitude IS NOT NULL
    AND gps_longitude != double precision 'NaN'
    AND gps_satellites >= 4
GROUP BY powerwatch.core_id, st, site_id
ORDER BY dist_diff_meters DESC
    ;




-- 


\COPY (SELECT     powerwatch.core_id,     st,     max(time) as maxtime,     min(time) as mintime,     count(*) as N_gps_samples,     avg(gps_satellites) as gps_satellites_avg,     avg(gps_latitude) AS lat_avg,     max(lat_fixed) as lat_fixed,     avg(gps_longitude) AS lon_avg,     max(lon_fixed) as lon_fixed,     110.25 * 1000 * sqrt(pow(avg(gps_latitude)-max(lat_fixed),2) + 	pow(avg(gps_longitude)-max(lon_fixed),2)) AS dist_diff_meters FROM     powerwatch     INNER JOIN (     SELECT core_id, 	location_latitude as lat_fixed, 	location_longitude as lon_fixed, 	COALESCE(deployment_start_time, '1970-01-01 00:00:00+0') as st, 	COALESCE(deployment_end_time, '9999-01-01 00:00:00+0') as et     FROM deployment 	) d ON powerwatch.core_id = d.core_id WHERE     time >= date '2018-07-01'     AND time <= date '2019-09-01'     AND time >= st     AND time < et  AND gps_longitude IS NOT NULL     AND gps_longitude != double precision 'NaN'     AND gps_satellites >= 4 GROUP BY powerwatch.core_id, st ORDER BY dist_diff_meters DESC ) TO '~/data/csv/2019-10-17-powerwatch-gps-locations-sats-ge-4.csv' CSV HEADER


-- relax the constraint and just find all core_id,start_time pairs where gps_satellites != 'NaN'
\COPY (SELECT     powerwatch.core_id,     st,     max(time) as maxtime,     min(time) as mintime,     count(*) as N_gps_samples,     avg(gps_satellites) as gps_satellites_avg,     avg(gps_latitude) AS lat_avg,     max(lat_fixed) as lat_fixed,     avg(gps_longitude) AS lon_avg,     max(lon_fixed) as lon_fixed,     110.25 * 1000 * sqrt(pow(avg(gps_latitude)-max(lat_fixed),2) + 	pow(avg(gps_longitude)-max(lon_fixed),2)) AS dist_diff_meters FROM     powerwatch     INNER JOIN (     SELECT core_id, 	location_latitude as lat_fixed, 	location_longitude as lon_fixed, 	COALESCE(deployment_start_time, '1970-01-01 00:00:00+0') as st, 	COALESCE(deployment_end_time, '9999-01-01 00:00:00+0') as et     FROM deployment 	) d ON powerwatch.core_id = d.core_id WHERE     time >= date '2018-07-01'     AND time <= date '2019-09-01'     AND time >= st     AND time < et  AND gps_satellites != 'NaN' GROUP BY powerwatch.core_id, st ORDER BY dist_diff_meters DESC ) TO '~/data/csv/2019-10-17-powerwatch-gps-locations-sats-not-NaN.csv' CSV HEADER
