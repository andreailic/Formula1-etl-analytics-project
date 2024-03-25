use Formula


if object_id ('Formula.Race','U') is not null
	drop table Formula.Race;
go

if object_id ('Formula.Status','U') is not null
	drop table Formula.Status;
go


if object_id ('Formula.DateDimension','U') is not null
	drop table Formula.DateDimension;
go


if object_id ('Formula.Driver','U') is not null
	drop table Formula.Driver;
go

if object_id ('Formula.driverStandings','U') is not null
	drop table Formula.driverStandings;
go

if object_id ('Formula.Team','U') is not null
	drop table Formula.Team;
go


if object_id ('Formula.teamStandings','U') is not null
	drop table Formula.teamStandings;
go


if object_id ('Formula.Location','U') is not null
	drop table Formula.Location;
go


if object_id ('Formula.Sprint','U') is not null
	drop table Formula.Sprint;
go

if object_id ('Formula.Pitstop','U') is not null
	drop table Formula.Pitstop;
go


if object_id('Formula.SEQ_pits_id','SO') is not null
	drop sequence Formula.SEQ_pits_id;
go



if object_id ('Formula.Laps','U') is not null
	drop table Formula.Laps;
go

if object_id('Formula.SEQ_laps_id','SO') is not null
	drop sequence Formula.SEQ_laps_id;
go




if schema_id ('Formula') is not null
	drop schema Formula;
go
create schema Formula;
go



create sequence Formula.SEQ_laps_id as int
start with 1
increment by 1
no cycle
go



create sequence Formula.SEQ_pits_id as int
start with 1
increment by 1
no cycle
go


CREATE TABLE TeamStandings (
  constructorStandingsId int not null primary key,
  constructorId integer,
  race_id integer,
  points_constructorstandings integer,
  position_constructorstandings integer,
  wins_constructorstandings integer,
  FOREIGN KEY (race_id) REFERENCES Race(race_id),
  FOREIGN KEY (constructorId) REFERENCES Team(constructorId)
)

CREATE TABLE Team (
  constructorId integer primary key,
  name_team varchar(30),
  constructorRef VARCHAR (255),
  nationality_constructors VARCHAR(30),
  url_constructors VARCHAR(MAX)
)


CREATE TABLE DriverStandings (
  driverStandingsId int not null primary key,
  raceId integer,
  driverId integer,
  points_driverstandings integer,
  position_driverstandings integer,
  wins integer,
   FOREIGN KEY (raceId) REFERENCES Race(race_id),
   FOREIGN KEY (driverId) REFERENCES Driver(driverId)

)



CREATE TABLE Driver (
  driverId integer not null PRIMARY KEY,
  driverRef nvarchar(255),
  constructorRef nvarchar(255),
  number integer,
  code nvarchar(255),
  forename nvarchar(255),
  surname nvarchar(255),
  dob date,
  nationality nvarchar(255),
  url_driver nvarchar(255),
  age int
);

/*
 DROP TRIGGER tr_CheckConstructorRef
CREATE TRIGGER tr_CheckConstructorRef
ON Driver
INSTEAD OF INSERT
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1
        FROM inserted i
        LEFT JOIN Team t ON i.constructorRef = t.constructorRef
        WHERE t.constructorRef IS NULL
    )
    BEGIN
        THROW 50000, 'Invalid constructorRef. Must exist in Team table.', 1;
    END
    ELSE
    BEGIN
        -- Ažurirajte ime stored procedure po potrebi
        EXEC dbo.sql_query;
    END
END;

*/


CREATE TABLE StatusDimension (
  statusId integer PRIMARY KEY,
  statusDescription nvarchar(255),

)




 CREATE TABLE LocationDimension (
  locationId int not null,
  name_loc VARCHAR(max),
  circuitRef nvarchar(255),
  location VARCHAR(max),
  country VARCHAR(max),
  lat FLOAT,
  lng FLOAT,
  url_location VARCHAR(max),
  PRIMARY KEY (locationId)
)



CREATE TABLE DateDimension (
  date DATE primary key not null,
  day INT,
  month INT,
  year INT
  )



CREATE TABLE Race (
  race_id int not null primary key,
  date date,
  round integer,
  locationId integer,
  FOREIGN KEY (locationId) REFERENCES LocationDimension(locationId),
  FOREIGN KEY (date) REFERENCES DateDimension(date)
);



 

CREATE TABLE Sprint (
  raceId integer NOT NULL primary key,
  sprint_date date,
  sprint_time time,
  FOREIGN KEY (raceId) REFERENCES Race(race_id)
);


CREATE TABLE Qualification (
  race_id integer not null,
  driver_id integer not null,
  quali_date date null,
  quali_time time,
  position integer,
  FOREIGN KEY (race_id) REFERENCES Race(race_id),
  FOREIGN KEY (driver_id) REFERENCES Driver(driverId)
);

-- Kreiranje indeksa na Qualification tabeli
CREATE INDEX idx_Qualification_driver_race_quali_date
ON Qualification (driver_id, race_id, quali_date);

CREATE TABLE Laps (
  lapsId int not null constraint DFT_laps_laps_id default(next value for Formula.SEQ_laps_id),
  raceId integer NOT NULL,
  driver_id integer not null,
  laps integer,
  lap integer,
  time_laptimes time,
  position_laptimes integer,
  milliseconds_laptimes integer,
  FOREIGN KEY (raceId) REFERENCES Race(race_id),
  FOREIGN KEY (driver_id) REFERENCES Driver(driverId)
)

CREATE INDEX IX_PitStop_RaceDriverStop
ON PitStop (race_id, driver_id, stop_number);


CREATE TABLE PitStop (
 pitsId int not null constraint DFT_pits_pits_id default(next value for Formula.SEQ_pits_id),
  race_id integer,
  driver_id integer,
  stop_number integer,
  lap_pitstops INT,
  time_pitstops TIME,
  duration float,
  milliseconds_pitstops INT,
  FOREIGN KEY (race_id) REFERENCES Race(race_id),
  FOREIGN KEY (driver_id) REFERENCES Driver(driverId)

)

CREATE TABLE Results (
  resultId INTEGER primary key,
  raceId INTEGER NOT NULL,
  driverId INTEGER NOT NULL,
  constructorId integer not null,
  position_order INTEGER,
  points float,
  laps INTEGER,
  rank INTEGER,
  fastestLap INTEGER,
  fastestLapTime TIME,
  fastestLapSpeed FLOAT,
  statusId integer,
  grid integer,
  FOREIGN KEY (raceId) REFERENCES Race(race_id),
  FOREIGN KEY (driverId) REFERENCES Driver(driverId),
  FOREIGN KEY (constructorId) REFERENCES Team(constructorId),
  FOREIGN KEY (statusId) REFERENCES StatusDimension(statusId)

);


CREATE TABLE FreePractice(
	raceId integer not null primary key,
	fp1_date date,
	fp1_time time,
	fp2_date date,
	fp2_time time,
	fp3_date date,
	fp3_time time,
	FOREIGN KEY (raceId) REFERENCES Race(race_id))



	CREATE TABLE TimeDimension(
	raceId integer not null primary key,
	race_duration time,
	start_time time,
	FOREIGN KEY (raceId) REFERENCES Race(race_id))




select * from StatusDimension

select * from DateDimension

select * from TeamStandings

select * from DriverStandings

select * from Driver

select * from LocationDimension

select * from Qualification

select * from Team

select * from Laps

select * from Pitstop

select * from Sprint

select * from FreePractice

select * from Race

select * from Results

select * from TimeDimension


delete from Laps
delete from TimeDimension
delete from FreePractice
delete from Sprint
delete from Results
delete from TeamStandings
delete from DriverStandings
delete from Pitstop
delete from Qualification
delete from Race
delete from DateDimension
delete from Team
delete from StatusDimension
delete from LocationDimension
delete from Driver








CREATE TABLE [CircuitLocation] (
  [circuit_key] integer PRIMARY KEY,
  [circuit_short_name] nvarchar(255),
  [country_code] nvarchar(255),
  [country_key] integer,
  [country_name] nvarchar(255)
)
GO