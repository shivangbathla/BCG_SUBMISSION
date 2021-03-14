import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, _}
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import org.apache.spark.sql.expressions.Window
import com.typesafe.config._

object BCGAnalysis  extends App {
  val props=ConfigFactory.load()
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder()
    .appName(props.getString("applicationName"))
    .config("spark.master",props.getString("executionMode"))
    .getOrCreate()

  val path=args(0)


  val unitsSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("UNIT_NBR",IntegerType),
    StructField("UNIT_DESC_ID",StringType),
    StructField("VEH_PARKED_FL",StringType),
    StructField("VEH_HNR_FL",StringType),
    StructField("VEH_LIC_STATE_ID",StringType),
    StructField("VIN",StringType),
    StructField("VEH_MOD_YEAR",StringType),
    StructField("VEH_COLOR_ID",StringType),
    StructField("VEH_MAKE_ID",StringType),
    StructField("VEH_MOD_ID",StringType),
    StructField("VEH_BODY_STYL_ID",StringType),
    StructField("EMER_RESPNDR_FL",StringType),
    StructField("OWNR_ZIP",StringType),
    StructField("FIN_RESP_PROOF_ID",StringType),
    StructField("FIN_RESP_TYPE_ID",StringType),
    StructField("VEH_DMAG_AREA_1_ID",StringType),
    StructField("VEH_DMAG_SCL_1_ID",StringType),
    StructField("FORCE_DIR_1_ID",StringType),
    StructField("VEH_DMAG_AREA_2_ID",StringType),
    StructField("VEH_DMAG_SCL_2_ID",StringType),
    StructField("FORCE_DIR_2_ID",StringType),
    StructField("VEH_INVENTORIED_FL",StringType),
    StructField("VEH_TRANSP_NAME",StringType),
    StructField("VEH_TRANSP_DEST",StringType),
    StructField("CONTRIB_FACTR_1_ID",StringType),
    StructField("CONTRIB_FACTR_2_ID",StringType),
    StructField("CONTRIB_FACTR_P1_ID",StringType),
    StructField("VEH_TRVL_DIR_ID",StringType),
    StructField("FIRST_HARM_EVT_INV_ID",StringType),
    StructField("INCAP_INJRY_CNT",IntegerType),
    StructField("NONINCAP_INJRY_CNT",IntegerType),
    StructField("POSS_INJRY_CNT",IntegerType),
    StructField("NON_INJRY_CNT",IntegerType),
    StructField("UNKN_INJRY_CNT",IntegerType),
    StructField("TOT_INJRY_CNT",IntegerType),
    StructField("DEATH_CNT",IntegerType)
  ))

  val primaryPersonSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("UNIT_NBR",IntegerType),
    StructField("PRSN_NBR",IntegerType),
    StructField("PRSN_TYPE_ID",StringType),
    StructField("PRSN_OCCPNT_POS_ID",StringType),
    StructField("PRSN_INJRY_SEV_ID",StringType),
    StructField("PRSN_AGE",StringType),
    StructField("PRSN_ETHNICITY_ID",StringType),
    StructField("PRSN_GNDR_ID",StringType),
    StructField("PRSN_EJCT_ID",StringType),
    StructField("PRSN_REST_ID",StringType),
    StructField("PRSN_AIRBAG_ID",StringType),
    StructField("PRSN_HELMET_ID",StringType),
    StructField("PRSN_SOL_FL",StringType),
    StructField("PRSN_ALC_SPEC_TYPE_ID",StringType),
    StructField("PRSN_ALC_RSLT_ID",StringType),
    StructField("PRSN_BAC_TEST_RSLT",StringType),
    StructField("PRSN_DRG_SPEC_TYPE_ID",StringType),
    StructField("PRSN_DRG_RSLT_ID",StringType),
    StructField("DRVR_DRG_CAT_1_ID",StringType),
    StructField("PRSN_DEATH_TIME",StringType),
    StructField("INCAP_INJRY_CNT",IntegerType),
    StructField("NONINCAP_INJRY_CNT",IntegerType),
    StructField("POSS_INJRY_CNT",IntegerType),
    StructField("NON_INJRY_CNT",IntegerType),
    StructField("UNKN_INJRY_CNT",IntegerType),
    StructField("TOT_INJRY_CNT",IntegerType),
    StructField("DEATH_CNT",IntegerType),
    StructField("DRVR_LIC_TYPE_ID",StringType),
    StructField("DRVR_LIC_STATE_ID",StringType),
    StructField("DRVR_LIC_CLS_ID",StringType),
    StructField("DRVR_ZIP",StringType)
  ))

  val damagesSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("DAMAGED_PROPERTY",StringType)
  ))

  val chargesSchema = StructType(List(
    StructField("CRASH_ID",IntegerType),
    StructField("UNIT_NBR",IntegerType),
    StructField("PRSN_NBR",IntegerType),
    StructField("CHARGE",StringType),
    StructField("CITATION_NBR",StringType)
  ))

  val Primary_Person=  spark.read
    .schema(primaryPersonSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Primary_Person_use.csv")

  val Units_use=  spark.read
    .schema(unitsSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Units_use.csv")


  val Damages_use=spark.read
    .schema(damagesSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Damages_use.csv")

  val Charges_use=spark.read
    .schema(chargesSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("sep",",")
    .csv(s"$path/Charges_use.csv")


  //Primary_Person.printSchema()
  //Primary_Person.show()

  //1.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male

  val Killed_males= Primary_Person.where(col("PRSN_INJRY_SEV_ID")==="KILLED" and col("PRSN_GNDR_ID")==="MALE")
  Killed_males.select(count("*") as "Killed_Males").write.mode(SaveMode.Overwrite).csv(s"$path/killed_males.csv")

  //3.	Analysis 3: Which state has highest number of accidents in which females are involved?
  val female_accidents= Primary_Person.where(col("PRSN_GNDR_ID")==="FEMALE").groupBy(col("DRVR_LIC_STATE_ID")).agg(count("*") as "female_count_per_state").orderBy(desc("female_count_per_state"))
  female_accidents.drop("female_count_per_state").limit(1).write.mode(SaveMode.Overwrite).csv(s"$path/highest_state.csv")

  //2.	Analysis 2: How many two wheelers are booked for crashes?
  val regexString = "MOTORCYCLE"
  //selecting records where two wheelers are involved
  val two_wheelers= Units_use.select(
    col("*"),
    regexp_extract(col("VEH_BODY_STYL_ID"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  //printing two wheelers count
  two_wheelers.select(count(("*")) as "tow_wheelers_count").write.mode(SaveMode.Overwrite).csv(s"$path/twowheelercounts.csv")

  //Analysis:4 the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

  //calculating total injury and totaly death count per VEH_MAKE_ID
  val inj_and_death_per_vechile=Units_use.groupBy(col("VEH_MAKE_ID"))
    .agg(sum(col("TOT_INJRY_CNT")).as("total_injury"),
      sum(col("DEATH_CNT")).as("total_death")
    )
  //creating new column as sum total no of deaths and injuries
  val Total_injury_incl_death_per_make_id=inj_and_death_per_vechile.withColumn("Total_injury_including_death",col("total_injury")+ col("total_death")).drop("total_injury","total_death")
  val WindowRank=Window.orderBy("Total_injury_including_death")

  //ranking data as per totalcount of death and injury in descending order
  val ranked_data=Total_injury_incl_death_per_make_id.withColumn("Highest_rank_per_total_count",dense_rank() over(WindowRank))

  //filtering rank between 5 to 15
  ranked_data.select("VEH_MAKE_ID").where(col("Highest_rank_per_total_count") >=5 and  col("Highest_rank_per_total_count") <=15)
    .write.mode(SaveMode.Overwrite).csv(s"$path/vehicle_make_id.csv")

  //`5. Analysis:5 For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

  //selecting PRSN_ENTHINICITY and VEH_BODY_STYL_ID by joining Primary_person and  UNITS_USE dataset
  val joinedDf=Primary_Person.join(Units_use,Primary_Person.col("CRASH_ID")===Units_use.col("CRASH_ID")).drop(Units_use.col("CRASH_ID"))
    .select("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID")
  // Grouping the data on Vehiclebody style and ethinic_id to get count of count of each ethinic group per Vehiclebody style
  val countPerBodyStylePerEthnicity= joinedDf.groupBy(col("VEH_BODY_STYL_ID"),col("PRSN_ETHNICITY_ID")).agg(count("*") as "count_per_ethicity_id_per_body_style").orderBy("VEH_BODY_STYL_ID")
  countPerBodyStylePerEthnicity
  val windowCount= Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(desc("count_per_ethicity_id_per_body_style"))
  //selecting highest ethinic group for each vehicle style
  countPerBodyStylePerEthnicity.withColumn("decreasingOrderOfEthnicityperbodyStyle",row_number() over(windowCount)).where(col("decreasingOrderOfEthnicityperbodyStyle")===1).select("PRSN_ETHNICITY_ID","VEH_BODY_STYL_ID")
    .write.mode(SaveMode.Overwrite).csv(s"$path/Highest_Vechile_body_style.csv")


  //6 Analysis:6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
  val regexString3="ALCOHOL"
  //filtering crashes in which alochol was involved
  val FilteredAlcoholCrashes= Units_use.select(
    col("*"),
    regexp_extract(col("CONTRIB_FACTR_1_ID"), regexString3, 0).as("regex_extract"),
    regexp_extract(col("CONTRIB_FACTR_2_ID"), regexString3, 0).as("regex_extract1"),
    regexp_extract(col("CONTRIB_FACTR_P1_ID"), regexString3, 0).as("regex_extract2")
  ).where(col("regex_extract") =!= "" or col("regex_extract1") =!= "" or col("regex_extract2") =!= "" ).drop("regex_extract","regex_extract1","regex_extract2")

  // joining output of filtered crashes(from unit_use dataset) with primary_perwson Datset to get DRIVER_ZIP
  val joinedDf1=Primary_Person.join(FilteredAlcoholCrashes,Primary_Person.col("CRASH_ID")===FilteredAlcoholCrashes.col("CRASH_ID")).drop(Primary_Person.col("CRASH_ID"))
  //counting no of crashes per DRIVERZIP
  val countPerDriverZip=joinedDf1.groupBy((col("DRVR_ZIP"))).agg(count("*").as("CountOfCrashes"))
  val windowCountRank=Window.orderBy(desc("CountOfCrashes"))
  //dropping  null driverzips and  selecting top 5 zips having highest crashes
  countPerDriverZip.na.drop.withColumn("rank_highest_count_first", dense_rank() over (windowCountRank)).where(col("rank_highest_count_first") <=5 ).select("DRVR_ZIP","CountOfCrashes")
    .write.mode(SaveMode.Overwrite).csv(s"$path/top5zipcodes.csv")


  //Analysis7 :Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

  //getting crashes with no damage
  val CrashesWtihNoDamages=Units_use.join(Damages_use,Damages_use.col("CRASH_ID")===Units_use.col("CRASH_ID"),"left_anti")
  val regexString4="INSURANCE"
  //filtering the crashes where car avails Insurance and VEHDAMG SCL >4
  val FilteredDf=CrashesWtihNoDamages.select(
    col("*"),
    regexp_extract(col("FIN_RESP_TYPE_ID"), regexString4, 0).as("regex_extract"),
  ).where(col("regex_extract") =!= "" and substring(col("VEH_DMAG_SCL_1_ID"),9,1) > 4)

  //count of distinct crashid
  FilteredDf.dropDuplicates("CRASH_ID").select(count(col("CRASH_ID")).as("conuntOfDistinctCrashes"))
    .write.mode(SaveMode.Overwrite).csv(s"$path/noOfCrashes.csv")


  //8 : Analytics 8:Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

  //filtering speeding charges
  val speedOffences = Charges_use.withColumn("OffenceType",regexp_extract(col("CHARGE"),"SPEED",0)).filter(col("OffenceType") =!= "")

  //filtering person with Driver licenses
  val licensedDrivers = Primary_Person.withColumn("IsLicensed",regexp_extract(col("DRVR_LIC_TYPE_ID"),"DRIVER LIC",0)).filter(col("IsLicensed") =!= "")
  //Vechicle with top 10 colours
  val top10colors = Units_use.groupBy(col("VEH_COLOR_ID")).agg(count("*").as("ColorCount")).orderBy(col("ColorCount").desc).limit(10)
  val top25states = Charges_use.groupBy(col("CRASH_ID")).agg(count("*").as("OffenceCount"))
    .join(Units_use.withColumn("VehicleType",regexp_extract(col("VEH_BODY_STYL_ID"),"CAR",0)).filter(col("VehicleType") =!= "").select("CRASH_ID","VEH_LIC_STATE_ID").filter(col("VEH_LIC_STATE_ID") =!= "NA"),"CRASH_ID")
    .groupBy("VEH_LIC_STATE_ID").agg(sum("OffenceCount").as("StateWiseOffenceCount"))
    .withColumn("rank",row_number().over(Window.orderBy(col("StateWiseOffenceCount").desc)))
    .filter(col("rank") < 26)
  //left semi joining with base dataset as Unit_use dataset to get the matching records from left dataset only i.eunits_use
  Units_use.select("CRASH_ID","VEH_MAKE_ID").join(speedOffences, Units_use.col("CRASH_ID") === speedOffences.col("CRASH_ID"),"left_semi")
    .join(licensedDrivers, Units_use.col("CRASH_ID") === licensedDrivers.col("CRASH_ID"),"left_semi")
    .join(top10colors, Units_use.col("VEH_COLOR_ID") === top10colors.col("VEH_COLOR_ID"),"left_semi")
    .join(top25states, Units_use.col("VEH_LIC_STATE_ID") === top25states.col("VEH_LIC_STATE_ID"),"left_semi")
    .groupBy(col("VEH_MAKE_ID")).agg(count("*").as("TotalCount")).orderBy(col("TotalCount").desc).limit(5).write.mode(SaveMode.Overwrite).csv(s"$path/top5makes.csv")
}
