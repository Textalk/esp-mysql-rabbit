CREATE TABLE events (
  globalPosition  BIGINT UNSIGNED NOT NULL,
  streamId        VARCHAR(100)    NOT NULL,
  eventNumber     BIGINT UNSIGNED NOT NULL,
  eventId         VARBINARY(16)   NOT NULL,
  eventType       VARCHAR(100)    NOT NULL,
  updated         DATETIME(6)     NOT NULL,
  data            TEXT, -- JSON,
  UNIQUE KEY `globalPosition` (`globalPosition`),
  UNIQUE KEY `streamEventNumber` (`streamId`, `eventNumber`),
  UNIQUE KEY `eventId` (`eventId`)
)
