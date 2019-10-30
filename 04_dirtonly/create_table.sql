CREATE TABLE uma_rating_04 
(
    Year char(4),
    MonthDay char(4),
    JyoCD char(2),
    Kaiji char(2),
    Nichiji char(2),
    RaceNum char(2),
    KettoNum char(10),
    Rating SMALLINT,
    PRIMARY KEY (Year, MonthDay, JyoCD, Kaiji, Nichiji, RaceNum, KettoNum)
);

