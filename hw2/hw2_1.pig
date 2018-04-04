data_2008 = LOAD '2008.csv' USING PigStorage(',') AS (
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
                                            WeatherDelay:chararray,
                                            NASDelay:chararray,
                                            SecurityDelay:chararray,
                                            LateAircraftDelay:chararray);
total_delay_2008 = FOREACH data_2008 GENERATE ArrDelay+DepDelay as TotalDelay:int;
grouped_total_delay = GROUP total_delay_2008 ALL;
limited_data = FOREACH grouped_total_delay GENERATE MAX(total_delay_2008.TotalDelay);
DUMP limited_data;
