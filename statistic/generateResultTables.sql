DELIMITER //
CREATE PROCEDURE generateResultTables(IN now DATETIME,IN timespan INT)
BEGIN
	DELETE FROM event_times WHERE time < DATE_SUB(now,INTERVAL 1 MONTH);
	INSERT INTO event_times SELECT * FROM (SELECT now) times,(SELECT event,COUNT(event) AS eventtimes FROM event_log_info WHERE time BETWEEN DATE_SUB(now,INTERVAL timespan SECOND) AND now GROUP BY event ORDER BY COUNT(event) DESC LIMIT 5) event_counts;  
	
	-- event ip iptimes 
	DELETE FROM event_ip_iptimes WHERE time < DATE_SUB(now,INTERVAL 1 MONTH);
	-- event,ip,iptimes last timespan second
	CREATE TABLE IF NOT EXISTS tmp_event_ip_iptimes ENGINE=MEMORY SELECT event,ip,count(ip) FROM event_log_info LIMIT 0; 
	TRUNCATE TABLE tmp_event_ip_iptimes;
	INSERT INTO tmp_event_ip_iptimes SELECT eventip.event,eventip.ip,count(*) as ipcounts FROM (SELECT topevents.event,topeventinfos.ip FROM (SELECT event FROM event_times WHERE time=now) topevents INNER JOIN (SELECT * FROM event_log_info WHERE time BETWEEN DATE_SUB(now,INTERVAL timespan SECOND) AND now) topeventinfos ON topevents.event=topeventinfos.event) eventip GROUP BY eventip.event,eventip.ip;
	INSERT INTO event_ip_iptimes SELECT * FROM (SELECT now) times,   
	(
		(select * from tmp_event_ip_iptimes where event=' MODSEC_ALERT_EVENT_LEAKAGE' order by 3 DESC LIMIT 5)
		UNION ALL
		(select * from tmp_event_ip_iptimes where event=' MODSEC_ALERT_EVENT_A' order by 3 DESC LIMIT 5)
		UNION ALL 
		(select * from tmp_event_ip_iptimes where event=' MODSEC_ALERT_EVENT_B' order by 3 DESC LIMIT 5) 
	)AS ret;


	-- event ruleid ruleid times
	DELETE FROM event_ruleid_ruleidtimes WHERE time < DATE_SUB(now,INTERVAL 1 MONTH);
	CREATE TABLE IF NOT EXISTS tmp_event_ruleid_ruleidtimes ENGINE=MEMORY SELECT event,ruleid,count(ruleid) FROM event_log_info LIMIT 0;
	TRUNCATE TABLE tmp_event_ruleid_ruleidtimes;
	INSERT INTO tmp_event_ruleid_ruleidtimes SELECT eventruleid.event,eventruleid.ruleid,count(*) as ruleidcounts FROM (SELECT topevents.event,topeventinfos.ruleid FROM (SELECT event FROM event_times WHERE time=now) topevents INNER JOIN (SELECT * FROM event_log_info WHERE time BETWEEN DATE_SUB(now,INTERVAL timespan SECOND) AND now) topeventinfos ON topevents.event=topeventinfos.event) eventruleid GROUP BY eventruleid.event,eventruleid.ruleid;
	INSERT INTO event_ruleid_ruleidtimes SELECT * FROM (SELECT now) times,
	(
		(SELECT * FROM tmp_event_ruleid_ruleidtimes WHERE event=' MODSEC_ALERT_EVENT_LEAKAGE' ORDER BY 3 DESC LIMIT 5)
		UNION ALL
		(SELECT * FROM tmp_event_ruleid_ruleidtimes WHERE event=' MODSEC_ALERT_EVENT_A' ORDER BY 3 LIMIT 5)
		UNION ALL
		(SELECT * FROM tmp_event_ruleid_ruleidtimes WHERE event=' MODSEC_ALERT_EVENT_B' ORDER BY 3 LIMIT 5)
	
	) AS ret_event_ruleid_ruleidtimes;
END
//
DELIMITER ;
