CREATE TABLE IF NOT EXISTS `uploads` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `eventDatetime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `eventAction` varchar(20) NOT NULL DEFAULT '',
  `callRef` int(11) NOT NULL,
  `eventValue` decimal(10,2) DEFAULT NULL,
  `eventCurrencyCode` varchar(3) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1;