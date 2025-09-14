CREATE TABLE IF NOT EXISTS time_dim (
    t_time_sk int,
    t_time_id text,
    t_time int,
    t_hour int,
    t_minute int,
    t_second int,
    t_am_pm text,
    t_shift text,
    t_sub_shift text,
    t_meal_time text,
    PRIMARY KEY (t_time_sk)
);
