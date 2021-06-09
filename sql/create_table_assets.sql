CREATE TABLE IF NOT EXISTS `assets` (
    `id` char(36) NOT NULL,
    `class` varchar(16) NOT NULL,
    `easy_to_borrow` tinyint(1) NOT NULL,
    `exchange` varchar(16) NOT NULL,
    `fractionable` tinyint(1) NOT NULL,
    `marginable` tinyint(1) NOT NULL,
    `name` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    `shortable` tinyint(1) NOT NULL,
    `status` varchar(16) NOT NULL,
    `symbol` varchar(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    `tradable` tinyint(1) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
