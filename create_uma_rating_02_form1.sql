CREATE TABLE uma_rating_02_form1
(
    Year char(4),
    MonthDay char(4),
    JyoCD char(2),
    Kaiji char(2),
    Nichiji char(2),
    RaceNum char(2),
    KettoNum char(10),

    RYear char(4),
    RMonthDay char(4),
    RJyoCD char(2),
    RKaiji char(2),
    RNichiji char(2),
    RRaceNum char(2),
    Rating SMALLINT,

    PRIMARY KEY (Year, MonthDay, JyoCD, Kaiji, Nichiji, RaceNum, KettoNum)
);

