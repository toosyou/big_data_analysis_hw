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

data = load_data('2007');
grouped_data = GROUP data by Origin;
ratio = FOREACH grouped_data {
    total = COUNT(data);
    delay_data = FILTER data BY (DepDelay > 0);
    GENERATE FLATTEN(group) as Origin, total as TotalCount, COUNT(delay_data) as DelayCount, (double)COUNT(delay_data)/(double)total as DelayRatio;
};
ratio_order = ORDER ratio BY DelayRatio DESC;
ratio_order = LIMIT ratio_order 5;
DUMP ratio_order;

delay_count_order = ORDER ratio BY DelayCount DESC;
delay_count_order = LIMIT delay_count_order 5;
DUMP delay_count_order
