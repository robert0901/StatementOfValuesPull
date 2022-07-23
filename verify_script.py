import pandas as pd
from datetime import datetime
from pandasql import sqldf
import pypyodbc
import sov_download
import xlrd
import os
import shutil
pd.set_option('display.max_columns', None)
today = datetime.today()

#data
#Call to scrape Duff and Phelps
verification_table=sov_download.sov_scrape()# scrape the data and verification table for validation
# Property File-----
property_dump_file_name = "I:/DW & Systems/American Appraisal/AA Downloads/current sov/Buildings.xls"
property_dump = pd.read_html(property_dump_file_name,header=0)[0]
property_dump[['MemberID','Campus_Site','BldgID']] = property_dump[['MemberID','Campus_Site','BldgID']].replace('\=|"|\s','',regex=True)
property_dump["DateofInspection"]=  pd.to_datetime(property_dump["DateofInspection"]).dt.normalize()

# Vehicle File-----
vehicle_dump_file_name = "I:/DW & Systems/American Appraisal/AA Downloads/current sov/VehiclesList.xls"
vehicle_dump = pd.read_html(vehicle_dump_file_name,header=0)[0]
vehicle_dump[['Member ID','VIN','Site ID']] = vehicle_dump[['Member ID','VIN','Site ID']].replace('\=|"|\s','',regex=True)
vehicle_dump["Date Acquired"]=  pd.to_datetime(vehicle_dump["Date Acquired"]).dt.normalize()
## Members inForce
server = 'TASB-RMSDW-PROD'
database = 'PCPolicy'
property_in_force_query = open('I:/DW & Systems/American Appraisal/AA Downloads/SQL Comparssion/property_in_force.sql','r')
auto_in_force_query=open('I:/DW & Systems/American Appraisal/AA Downloads/SQL Comparssion/auto_in_force.sql','r')
conn = pypyodbc.connect("""
    Driver={{SQL Server Native Client 11.0}};
    Server={0};
    Database={1};
    Trusted_Connection=yes;""".format(server , database)
)
property_in_force = pd.read_sql(property_in_force_query.read(), conn) #Property in force
vehicle_in_force = pd.read_sql(auto_in_force_query.read(), conn) # Vehicles in force
wind_county = pd.read_sql("select CDNumber,County,NWSTerritory from Service.dbo.tgtMultiUseBookOfBusiness",conn) # county and wind_county territories

# # # # ##Property Verification
# # 1.Scope of Service
errors_df_property=property_dump[property_dump["ScopeofService"].isnull()].assign(ErrorType = "Property-Scope of Service")
# # 2.YearBuilt-- >Year(TodaysDate()) or < 1846
errors_df_property=pd.concat([errors_df_property,property_dump[(property_dump["YearBuilt"] > float(today.year)) | (property_dump["YearBuilt"] < 1846 )].assign(ErrorType = "Property-Year Built")])
## 3. Alarm Errrors
errors_df_property = pd.concat(
    [
        errors_df_property,
        property_dump[
            property_dump["DateofInspection"].notnull()
            & property_dump[
                ["EntryAlarm", "ManualFireAlarm", "AutoFireAlarm", "AutoSprinkler"]].isnull().any(axis=1) & ~property_dump["BldgName"].str.lower().str.contains("land improvement")

        ].assign(ErrorType="Property-Alarm Error")
    ])
## 4. Protection Class-- Not Between 1 and 10
errors_df_property=pd.concat([errors_df_property,property_dump[(property_dump["ProtectionClass"] > 10) | (property_dump["ProtectionClass"] < 1)].assign(ErrorType = "Property-Protection Class Error")])
## 5. ISOClass to RoofPitchErrors
# ISOCLass isNull() AND (Building Cost of Reproduction <>0 and DateOfInspection Is Not Null And >#1/1/2008#)
# All others to RoofPitch Errors--  DateOfInspection Is Not Null And >#1/1/2008# and  All others IS NUll()
errors_df_property=pd.concat(
    [
        errors_df_property,
        property_dump[(property_dump["ISOClass"].isnull()) &
                                           ((property_dump["BuildingCostofReproductionNew"]!=0)  &
                                            (property_dump["DateofInspection"]>'1/1/2008')&
                                            ~property_dump["BldgName"].str.lower().str.contains("land improvement",regex=True))
    ].assign(ErrorType = "ISOClass Error")
])
errors_df_property = pd.concat(
    [
        errors_df_property,
        property_dump[
            ((property_dump["DateofInspection"]>pd.to_datetime('1/1/2008').strftime("%m/%d/%Y"))
            & property_dump[
                ["ExteriorWallType", "Heating", "Cooling", "RoofMaterial","RoofPitch"]].isnull().any(axis=1)) & ~property_dump["BldgName"].str.lower().str.contains("land improvement",regex=True)

        ].assign(ErrorType="Alarm Error, Error,WallType Error,Heating Error, Cooling Error, RoofMaterial Error, or RoofPitch Error")
    ])

## 6.Addresses
# IS NUll() Building Address1
errors_df_property=pd.concat([errors_df_property,property_dump[property_dump["BuildingAddress1"].isnull()].assign(ErrorType = "Property-BuildingAddress1")])
# Is NULL() BuildingCity
errors_df_property= pd.concat([errors_df_property,property_dump[property_dump["BuildingCity"].isnull()].assign(ErrorType = "Property-BuildingCity")])
# Is NULL() or <> "TX" BuildingState
errors_df_property= pd.concat([errors_df_property,property_dump[(property_dump["BuildingState"].isnull()) | (property_dump["BuildingState"]!="TX")].assign(ErrorType = "Property-BuildingState")])
# Is NULL() or not like '7####' BuildingZip
errors_df_property= pd.concat([errors_df_property,property_dump[(property_dump["BuildingZip"].isnull()) | (~property_dump["BuildingZip"].astype
(str).str.contains('^7\d\d\d\d', regex=True, na=True))].assign(ErrorType = "Property-BuildingZip")])
## 7. Latitude Errors
# (Date of Inspection-Is Not Null And >#1/1/2008#) AND BuildingCostofReproductionNew<>0 AND Latitude is Null
errors_df_property= pd.concat([errors_df_property,property_dump[(property_dump["Latitude_Dg"].isnull()) &
                                           ((property_dump["BuildingCostofReproductionNew"]!=0)  &
                                            (property_dump["DateofInspection"]>'1/1/2008'))].assign(ErrorType = "Property-Latitude Null Error")])

latitude=[
'N 24.+',
'N 23',
'N 22.+',
'N 21.+',
'N 20.+',
'N 1.+',
'N 37.+',
'N 38.+',
'N 39.+',
'N 4.+',
'N 5.+',
'N 6.+',
'N 7.+',
'N 8.+',
'N 9.+']
errors_df_property=pd.concat([errors_df_property,property_dump[property_dump["Latitude_Dg"].
                             astype(str).str.contains('|'.join(latitude),regex=True, na=True)].
                             assign(ErrorType = "Property-Latitude Out of Bounds Error")])

##7. Longitude Errors
#(Date of Inspection-Is Not Null And >#1/1/2008#) AND BuildingCostofReproductionNew<>0 AND Longitude is Null
errors_df_property=pd.concat([errors_df_property,property_dump[(property_dump["Longitude_Dg"].isnull()) &
                                           ((property_dump["BuildingCostofReproductionNew"]!=0)  &
                                            (property_dump["DateofInspection"]>'1/1/2008'))].assign(ErrorType = "Property-Longitude Null Error")])

longitude =[
'W 107.+',
'W 108.+',
'W 109.+',
'W 11.+',
'W 12.+',
'W 13.+',
'W 14.+',
'W 15.+',
'W 16.+',
'W 17.+',
'W 18.+',
'W 19.+',
'W 2.+',
'W 3.+',
'W 4.+',
'W 5.+',
'W 6.+',
'W 7.+',
'W 8.+',
'W 9.+',
'W 092.+',
'W 091.+',
'W 090.+',
'W 08.+',
'W 07.+',
'W 06.+',
'W 05.+',
'W 04.+',
'W 03.+',
'W 02.+',
'W 01.+',
'W 00.+']

errors_df_property=pd.concat( [errors_df_property,property_dump[property_dump["Longitude_Dg"].astype(str).
                              str.contains('|'.join(longitude),regex=True, na=True)].assign(ErrorType = "Property-Longitude Out of Bounds Error")])
                              


#  Distinct TASB ID check Property or Empty Record 
errors_df_property=pd.concat([errors_df_property,property_dump[property_dump["TASB_Unique_Id"].duplicated() | property_dump["TASB_Unique_Id"].isnull()].assign(ErrorType = "Property-TASB ID Duplicate")])

## 8. Merge with in Force
# In Force but not in Download
property_in_force_not_in_download=property_in_force[~property_in_force["cdnum"].isin(property_dump.MemberID)].drop_duplicates().assign(ErrorType = "In Force but not in Download")
# In Download but not in-force
property_in_download_not_in_force=property_dump[~property_dump["MemberID"].isin(property_in_force.cdnum)].drop_duplicates().assign(ErrorType = "In Download but not In Force")
#summary
total_square_feet = property_dump[["SquareFootageSuperSructure","SquareFootageSubStructure"]].sum(axis=1)
summary_table_property = pd.DataFrame()
summary_table_property=summary_table_property.assign(Total_Count=[property_dump[property_dump.columns[0]].count()])
summary_table_property=summary_table_property.assign(Total_Bldg_Value =[property_dump.BuildingCostofReproductionNew.sum(axis=0)])
summary_table_property=summary_table_property.assign(Total_Content_Value=[property_dump.ContentsCostofReproductionNew.sum(axis=0)])
summary_table_property=summary_table_property.assign(Total_LI_Value=[property_dump.LandImprovementsCostofReproductionNew.sum(axis=0)])
summary_table_property=summary_table_property.assign(Total_Insurable_Value =summary_table_property[["Total_Bldg_Value","Total_Content_Value","Total_LI_Value"]].sum(axis=1))
summary_table_property=summary_table_property.assign(Total_Sq_Ft=total_square_feet.agg("sum"))
summary_table_property=summary_table_property.assign(Vehicle_Count=[vehicle_dump[vehicle_dump.columns[0]].count()])

try:
# Comparison Table
    comparison_table = pd.DataFrame()
    comparison_table=comparison_table.assign(Total_Count=summary_table_property["Total_Count"]-verification_table["Total Count"])
    comparison_table=comparison_table.assign(Total_Bldg_Value=summary_table_property["Total_Bldg_Value"]-verification_table["Total Bldg Value"])
    comparison_table=comparison_table.assign(Total_Content_Value=summary_table_property["Total_Content_Value"]-verification_table["Total Content Value"])
    comparison_table=comparison_table.assign(Total_LI_Value=summary_table_property["Total_LI_Value"]-verification_table["Total LI Value"])
    comparison_table=comparison_table.assign(Total_Insurable_Value=summary_table_property["Total_Insurable_Value"]-verification_table["Total Insurable Value"])
    comparison_table=comparison_table.assign(Total_Sq_Ft=summary_table_property["Total_Sq_Ft"]-verification_table["Total Sq.Ft."])
    comparison_table=comparison_table.assign(Vehicle_Count=summary_table_property["Vehicle_Count"]-verification_table["Vehicle Count"])
except NameError:
    pass


### Vehicles
## 1. Make sure vehicle description does not differ from past description. Regex equal to yes.
VehicleDescription=[
"BB2 - BUS SEATING 21\+",
"CC1 - CAR \/ SUV",
"MM1 - MOBILE EQUIPMENT",
"TT1 - TRUCK \/ VAN \(0 - 5 TON\)",
"TT3 - TRAILER \/ MOTORCYCLE",
"EV1 - EMERGENCY OR HIGHLY MODIFIED SERVICE VEHICLE",
"TT2 - LARGE TRUCK 5\+ TONS",
"BB1 - BUS SEATING 1 - 20",
"TT3 - TRAILER \/MOTORCYCLE"]
errors_df_vehicle=vehicle_dump[~vehicle_dump["Description"].str.strip().str.contains('|'.join(VehicleDescription),regex=True)].assign(ErrorType = "Vehicle- Description Error")
## 2. Vehicle Year < 1937 or >Today's Year+2 -- can have 2023 model
errors_df_vehicle=pd.concat([errors_df_vehicle,vehicle_dump[(vehicle_dump["Year"] > float(today.year+2)) | (vehicle_dump["Year"] < 1937 )].assign(ErrorType = "Vehicle- Year Error")])
# 3. Date Acquired <>#1/1/1900# And (<#1/1/1957# Or >AsOfDate)
errors_df_vehicle=pd.concat([errors_df_vehicle,vehicle_dump[(vehicle_dump["Date Acquired"] != pd.to_datetime("1/1/1900").strftime("%m/%d/%Y")) &
                                                            ((vehicle_dump["Date Acquired"] > pd.to_datetime("today").strftime("%m/%d/%Y"))|
                                                             (vehicle_dump["Date Acquired"] <pd.to_datetime("1/1/1957").strftime("%m/%d/%Y") ))].assign(ErrorType = "Vehicle- Date Acquired Error")])
## 4.Vehicle Type Errors
# --Pull as dsitinct vector from past spreadsheet and comparison Regex equal to yes.
# ^|/b beginning of string and end of string. Used to add quotes
vehicle_type =[
"BB2",
"CC1",
"MM1",
"TT1",
"TT3",
"EV1",
"TT2",
"BB1"]
errors_df_vehicle=pd.concat([errors_df_vehicle,vehicle_dump[~vehicle_dump["Vehicle_Type"].str.contains('|'.join(vehicle_type),regex=True,na=True)].assign(ErrorType = "Vehicle- Type Error")])

errors_df_vehicle=pd.concat([errors_df_vehicle,vehicle_dump[vehicle_dump["Vehicle ID"].duplicated()].assign(ErrorType = "Vehicle-Vehicle ID Duplicate")])
# 5. Merge with in Force
# In Force but not in Download
vehicle_in_force_not_in_download = vehicle_in_force[~vehicle_in_force["cdnum"].isin(vehicle_dump["Member ID"])].drop_duplicates().assign(ErrorType = "In Force but not in Download")
vehicle_in_download_not_in_force=vehicle_dump[~vehicle_dump["Member ID"].isin(vehicle_in_force.cdnum)].drop_duplicates().assign(ErrorType = "In Download but not In Force")

#summary_table_vehicle=summary_table_vehicle.assign(Total_Purchase_Price=[vehicle_dump["Purchase Price"].sum(axis=0)]) can't verify?
# # Export clean Excel** This in reality can go straight to DW if all is correct. Longterm solution

with pd.ExcelWriter("error report.xlsx") as writer:
    # use to_excel function and specify the sheet_name and index
    # to store the dataframe in specified sheet
    comparison_table.to_excel(writer,sheet_name="compare", index=False)
    errors_df_vehicle.to_excel(writer, sheet_name="Vehicle Errors", index=False)
    errors_df_property.to_excel(writer, sheet_name="Property Errors", index=False)
    property_in_download_not_in_force.to_excel(writer, sheet_name="property_in_download", index=False)
    property_in_force_not_in_download.to_excel(writer, sheet_name="property_in_force", index=False)
    vehicle_in_force_not_in_download.to_excel(writer, sheet_name="vehicle_in_force", index=False)
    vehicle_in_download_not_in_force.to_excel(writer, sheet_name="vehicle_in_download", index=False)
   #summary_table_property.to_excel(writer, sheet_name="summary_table_property", index=False)
   # summary_table_vehicle.to_excel(writer, sheet_name="summary_table_vehicle", index=False)
#os.system('start excel.exe"error report.xlsx"')
os.system('start excel.exe "I:/DW & Systems/American Appraisal/AA Downloads/error report.xlsx"') # Open Error Report
if not (property_in_force_not_in_download.empty or property_in_download_not_in_force.empty):
    print("There is problem in download or not in force error in property.")
else:
    property_dump.insert(0, "AsofDate",today.strftime('%m/%d/%Y'))
    property_dump=property_dump.merge(wind_county,how="inner",left_on="MemberID",right_on="cdnumber").drop(["cdnumber","ESC_Region_Code","Roof_Age"],1)
    property_dump.rename({'GroupID': 'Expr1', 'ContractID': 'Expr2','ContractName': 'Expr3','nwsterritory':'Wind Territory','county':'County',
                          "Latitude_Dg":"Latitude","Longitude_Dg":"Longitude"}, axis=1, inplace=True)
    property_dump.insert(loc=57, column='Original_ID', value=0)
    property_dump["Previous_CRN"] = "999999"
    property_dump["Schedule_Date"]="1/1/1900"
    property_dump["Campus_Type"] = "None"
    address_id = property_dump['Address_ID']
    property_dump = property_dump.drop(columns=['Address_ID'])
    property_dump.insert(loc=61, column='Address_ID', value=address_id)



    with pd.ExcelWriter(
            f"Prepped Files/Property American Appraisal Dump {today.strftime('%m-%d-%Y')} (CLEAN).xlsx",datetime_format="m/d/yyyy"
    ) as writer:
        property_dump.to_excel(writer,sheet_name='Property_American_Appraisa_Dump',index=False)
    property_dump.to_csv("//rms-etl-prd-02/infa_shared/SrcFiles/sovbuilding.csv",mode='w',index=False)


if not (vehicle_in_force_not_in_download.empty or vehicle_in_download_not_in_force.empty):
    print("There is problem in download or not in force error in vehicle.")
else:
        vehicle_dump.insert(0, "AsofDate", today.strftime('%m/%d/%Y'))
        vehicle_dump.drop("Site ID",1,inplace=True)

        with pd.ExcelWriter(
                f"Prepped Files/Vehicles list {today.strftime('%m-%d-%Y')} (CLEAN).xlsx",datetime_format="m/d/yyyy"
        ) as writer:
            vehicle_dump.to_excel(writer,sheet_name="Vehicles_List_XX_XX_XX__Clean_", index=False)
        vehicle_dump.to_csv("//rms-etl-prd-02/infa_shared/SrcFiles/sovvehicle.csv",mode='w',index=False)
