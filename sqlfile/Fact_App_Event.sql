
CREATE TABLE Fact_App_Events (
    UserID INTEGER,
    SessionID INTEGER,
    Timestamp TEXT,         
    event_name TEXT,
    ProductID TEXT,
    Amount NUMERIC(18, 9),  
    Outcome TEXT
);



COMMENT ON TABLE Fact_App_Events IS 'Bảng Fact lưu trữ các sự kiện ứng dụng từ clickstream';
COMMENT ON COLUMN Fact_App_Events.event_name IS 'Tên sự kiện (được đổi tên từ EventType trong tệp CSV)';

SELECT * FROM Fact_App_Events LIMIT 10;

SELECT DISTINCT event_name FROM Fact_App_Events;

SELECT COUNT(*) FROM fact_app_events;

SELECT current_database();
