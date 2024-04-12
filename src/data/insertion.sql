-- Insertion dans les tables de dimensions
INSERT INTO
    Dim_Vendors (Name)
SELECT DISTINCT
    VendorName
FROM raw_trips ON CONFLICT (Name) DO NOTHING;

INSERT INTO
    Dim_Time (Date, Hour, DayOfWeek)
SELECT DISTINCT
    CAST(pickup_datetime AS DATE),
    EXTRACT(
        HOUR
        FROM pickup_datetime
    ),
    EXTRACT(
        DOW
        FROM pickup_datetime
    )
FROM raw_trips ON CONFLICT (Date, Hour, DayOfWeek) DO NOTHING;

INSERT INTO
    Dim_Locations (Borough, Zone)
SELECT DISTINCT
    PickupBorough,
    PickupZone
FROM raw_trips
UNION
SELECT DISTINCT
    DropoffBorough,
    DropoffZone
FROM raw_trips ON CONFLICT (Borough, Zone) DO NOTHING;

INSERT INTO
    Dim_PaymentTypes (Description)
SELECT DISTINCT
    PaymentType
FROM raw_trips ON CONFLICT (Description) DO NOTHING;

-- Insertion dans la table de faits
INSERT INTO
    Fact_Trips (
        VendorID, PickupTimeID, DropoffTimeID, PassengerCount, TripDistance, RateCodeID, StoreAndFwdFlag, PaymentTypeID, FareAmount, Extra, MtaTax, TipAmount, TollsAmount, ImprovementSurcharge, TotalAmount, PickupLocationID, DropoffLocationID
    )
SELECT
    v.VendorID,
    pt_pick.TimeID,
    pt_drop.TimeID,
    rt.PassengerCount,
    rt.TripDistance,
    rt.RateCodeID,
    rt.StoreAndFwdFlag,
    pt.PaymentTypeID,
    rt.FareAmount,
    rt.Extra,
    rt.MtaTax,
    rt.TipAmount,
    rt.TollsAmount,
    rt.ImprovementSurcharge,
    rt.TotalAmount,
    loc_pick.LocationID,
    loc_drop.LocationID
FROM
    raw_trips rt
    JOIN Dim_Vendors v ON rt.VendorName = v.Name
    JOIN Dim_Time pt_pick ON CAST(rt.pickup_datetime AS DATE) = pt_pick.Date
    AND EXTRACT(
        HOUR
        FROM rt.pickup_datetime
    ) = pt_pick.Hour
    JOIN Dim_Time pt_drop ON CAST(rt.dropoff_datetime AS DATE) = pt_drop.Date
    AND EXTRACT(
        HOUR
        FROM rt.dropoff_datetime
    ) = pt_drop.Hour
    JOIN Dim_Locations loc_pick ON rt.PickupBorough = loc_pick.Borough
    AND rt.PickupZone = loc_pick.Zone
    JOIN Dim_Locations loc_drop ON rt.DropoffBorough = loc_drop.Borough
    AND rt.DropoffZone = loc_drop.Zone
    JOIN Dim_PaymentTypes pt ON rt.PaymentType = pt.Description;