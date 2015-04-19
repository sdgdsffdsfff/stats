DELIMITER //
CREATE PROCEDURE createTables()
BEGIN
    -- if not exist create event_log_info
    CREATE TABLE IF NOT EXISTS event_log_info(num INT NOT NULL AUTO_INCREMENT PRIMARY KEY,time DATETIME NOT NULL,event VARCHAR(32) NOT NULL,ip CHAR(15) NOT NULL,ruleid INT(7) NOT NULL);

    -- if not exists create event_times 
    CREATE TABLE IF NOT EXISTS event_times SELECT time,event,COUNT(event) as eventtimes FROM event_log_info LIMIT 0;

    -- if not exists create event_ip_iptimes
    CREATE TABLE IF NOT EXISTS event_ip_iptimes SELECT time,event,ip,COUNT(ip) as iptimes FROM event_log_info LIMIT 0;

    -- if not exists create event_ruleid_ruleidtimes
    CREATE TABLE IF NOT EXISTS event_ruleid_ruleidtimes SELECT time,event,ruleid,COUNT(ruleid) AS ruleidtimes FROM event_log_info LIMIT 0;
END
//
DELIMITER ;
