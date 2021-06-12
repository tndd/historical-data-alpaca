CREATE TABLE IF NOT EXISTS `market_data_dl_progress` (
    `asset_id` char(36) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    `category` varchar(8) NOT NULL,
    `time_frame` varchar(8) NOT NULL,
    `until` datetime DEFAULT NULL,
    `message` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
    PRIMARY KEY (`asset_id`,`category`,`time_frame`),
    KEY `market_data_dl_progress_FK` (`asset_id`),
    CONSTRAINT `market_data_dl_progress_FK` FOREIGN KEY (`asset_id`) REFERENCES `assets` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
