-- Create national walkability index
DROP TABLE IF EXISTS wa_1600m_ntnl;
CREATE TABLE wa_1600m_ntnl AS
SELECT gnaf_pid,
       wa_sco_1600m_dl_2018,
       wa_dns_1600m_sc_2018,
       wa_dns_1600m_dd_2018,
       z_wa_sco_1600m_dl_2018,
       z_wa_dns_1600m_sc_2018,
       z_wa_dns_1600m_dd_2018,
       (z_wa_sco_1600m_dl_2018 + z_wa_dns_1600m_sc_2018 + z_wa_dns_1600m_dd_2018) AS wa_sco_1600m_national_2018
FROM (SELECT gnaf_pid, 
             wa_sco_1600m_dl_2018,
             wa_dns_1600m_sc_2018,
             wa_dns_1600m_dd_2018,
             (wa_sco_1600m_dl_2018 - AVG(wa_sco_1600m_dl_2018) OVER())/stddev_pop(wa_sco_1600m_dl_2018) OVER() as z_wa_sco_1600m_dl_2018,
             (wa_dns_1600m_sc_2018 - AVG(wa_dns_1600m_sc_2018) OVER())/stddev_pop(wa_dns_1600m_sc_2018) OVER() as z_wa_dns_1600m_sc_2018,
             (wa_dns_1600m_dd_2018 - AVG(wa_dns_1600m_dd_2018) OVER())/stddev_pop(wa_dns_1600m_dd_2018) OVER() as z_wa_dns_1600m_dd_2018 
      FROM aedc_indicators_aifs
      WHERE exclude IS NULL) t;
CREATE UNIQUE INDEX ix_wa_1600m_ntnl ON  wa_1600m_ntnl (gnaf_pid);  