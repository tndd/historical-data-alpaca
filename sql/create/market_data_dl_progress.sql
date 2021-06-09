CREATE TABLE IF NOT EXISTS `market_data_dl_progress` (
  `category` varchar(8) NOT NULL,
  `time_frame` varchar(8) NOT NULL,
  `until` datetime NOT NULL,
  `message` varchar(256) NOT NULL,
  `symbol_id` char(36) NOT NULL,
  PRIMARY KEY (`category`,`time_frame`),
  KEY `market_data_dl_progress_FK` (`symbol_id`),
  CONSTRAINT `market_data_dl_progress_FK` FOREIGN KEY (`symbol_id`) REFERENCES `assets` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
