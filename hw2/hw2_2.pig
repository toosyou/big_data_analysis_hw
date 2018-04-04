DEFINE load_data(year) RETURNS data_year{
    $data_year = LOAD '$year.csv' USING PigStorage(',') AS (
                                                Year:int,
                                                Month:int,
                                                DayofMonth:int,
                                                DayofWeek:int,
                                                DepTime:int,
                                                CRSDepTime:int,
                                                ArrTime:int,
                                                CRSArrTime:int,
                                                UniqueCarrier:chararray,
                                                FlightNum:int,
                                                TailNum:chararray,
                                                ActualElapsedTime:int,
                                                CRSElapsedTime:int,
                                                AirTime:int,
                                                ArrDelay:int,
                                                DepDelay:int,
                                                Origin:chararray,
                                                Dest:chararray,
                                                Distance:int,
                                                TaxiIn:chararray,
                                                TaxiOut:chararray,
                                                Cancelled:chararray,
                                                CancellationCode:chararray,
                                                Diverted:chararray,
                                                CarrierDelay:chararray,
                                                WeatherDelay:int,
                                                NASDelay:chararray,
                                                SecurityDelay:chararray,
                                                LateAircraftDelay:chararray);
};

data = load_data('{200[0-5]}');
filtered_data = FILTER data BY (WeatherDelay > 0);
grouped_data = GROUP filtered_data by Year;
DESCRIBE grouped_data;
count = FOREACH grouped_data GENERATE group, COUNT(filtered_data);
DESCRIBE count;
DUMP count;
