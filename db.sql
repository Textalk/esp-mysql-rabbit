CREATE TABLE events (
  streamId     VARCHAR(100),
  eventNumber  BIGINT UNSIGNED,
  eventId      VARBINARY(16),
  eventType    VARCHAR(100),
  created      DATETIME,
  data         TEXT, -- JSON,
  UNIQUE KEY `streamEventNumber` (`streamId`, `eventNumber`),
  UNIQUE KEY `eventId` (`eventId`)
)
