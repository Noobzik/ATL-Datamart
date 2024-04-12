-- Creation des tables de dimensions
CREATE TABLE Dim_Vendors (
    VendorID SERIAL PRIMARY KEY, Name VARCHAR(255)
);

CREATE TABLE Dim_Time (
    TimeID SERIAL PRIMARY KEY, Date DATE, Hour INT, DayOfWeek INT
);

CREATE TABLE Dim_Locations (
    LocationID SERIAL PRIMARY KEY, Borough VARCHAR(255), Zone VARCHAR(255)
);

CREATE TABLE Dim_PaymentTypes (
    PaymentTypeID SERIAL PRIMARY KEY, Description VARCHAR(255)
);

-- Creation de la table de faits
CREATE TABLE Fact_Trips (
    TripID SERIAL PRIMARY KEY, VendorID INT REFERENCES Dim_Vendors (VendorID), PickupTimeID INT REFERENCES Dim_Time (TimeID), DropoffTimeID INT REFERENCES Dim_Time (TimeID), PassengerCount INT, TripDistance FLOAT, RateCodeID INT, StoreAndFwdFlag CHAR(1), PaymentTypeID INT REFERENCES Dim_PaymentTypes (PaymentTypeID), FareAmount MONEY, Extra MONEY, MtaTax MONEY, TipAmount MONEY, TollsAmount MONEY, ImprovementSurcharge MONEY, TotalAmount MONEY, PickupLocationID INT REFERENCES Dim_Locations (LocationID), DropoffLocationID INT REFERENCES Dim_Locations (LocationID)
);