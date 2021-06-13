CREATE TABLE IF NOT EXISTS `bars_1min` (
    `time` datetime NOT NULL,
    `symbol` char(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    `open` double NOT NULL,
    `high` double NOT NULL,
    `low` double NOT NULL,
    `close` double NOT NULL,
    `volume` int unsigned NOT NULL,
    PRIMARY KEY (`time`,`symbol`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
