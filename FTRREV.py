import datetime as dt
import os
import numpy as np
import pandas as pd
import glob

def MPMA(year : int) -> pd.DataFrame:
    print("Processing the ", year)
    bigBoysPlayers  = [] 
    yr              = str(year)[2:]
    months = { 
               "Jun"+yr                 : (str(year)+"-06-01", str(year)+"-06-30"),
               "Jul"+yr                 : (str(year)+"-07-01", str(year)+"-07-31"),
               "Aug"+yr                 : (str(year)+"-08-01", str(year)+"-08-31"), 
               "Sep"+yr                 : (str(year)+"-09-01", str(year)+"-09-30"), 
               "Oct"+yr                 : (str(year)+"-10-01", str(year)+"-10-31"), 
               "Nov"+yr                 : (str(year)+"-11-01", str(year)+"-11-30"), 
               "Dec"+yr                 : (str(year)+"-12-01", str(year)+"-12-31"), 
               "Jan"+ str(year+1)[-2:]  : (str(year+1)+"-01-01", str(year+1)+"-01-31"),
               "Feb"+ str(year+1)[-2:]  : (str(year+1)+"-02-01", str(year+1)+"-02-28"),
               "Mar"+ str(year+1)[-2:]  : (str(year+1)+"-03-01", str(year+1)+"-03-31"),
               "Apr"+ str(year+1)[-2:]  : (str(year+1)+"-04-01", str(year+1)+"-04-30"),
               "May"+ str(year+1)[-2:]  : (str(year+1)+"-05-01", str(year+1)+"-05-31")
            }
    file_name_template =  "dam_data_{}.csv"
    for month in months: 
        start_date  = dt.datetime.strptime(months[month][0] , "%Y-%m-%d")
        end_date    = dt.datetime.strptime(months[month][1], "%Y-%m-%d")
        total_days  = (end_date - start_date).days + 1
        totalLMP_SCR_SINK = {}
        SCR_SINK_TYPE = {}
        print("Calculating the LMP sum for each source and sink - ", month)
        for i in range(total_days):
            print("DAM sum progress: ", np.round((i/total_days)*100, 2), "%", end="\r")
            _date       = start_date + dt.timedelta(days=i)
            _file_name  = file_name_template.format(_date.strftime("%Y%m%d"))
            csv_path    =  "/Users/jeffreychan/Documents/school/UCR/PhD/Yu/data/" + _date.strftime("%Y") + "/dam/" + _date.strftime("%Y") + "/"
            _file_path  = os.path.join(csv_path, _file_name)
            df          = pd.read_csv(_file_path, skiprows = 4)
            df          = df[df.Value == "LMP"].reset_index()
            for dataRow in range(len(df)):
                row = df.iloc[dataRow]
                if row["Node"] in totalLMP_SCR_SINK:
                    totalLMP_SCR_SINK[row["Node"]][0] += row[df.columns[10:-2]].sum()
                    totalLMP_SCR_SINK[row["Node"]][1] += row[df.columns[4:10]].sum()+row[df.columns[-2:]].sum()
                else:
                    SCR_SINK_TYPE[row["Node"]] = row["Type"]
                    totalLMP_SCR_SINK[row["Node"]] = [row[df.columns[10:-2]].sum(), (row[df.columns[4:10]].sum()+row[df.columns[-2:]].sum())]                 
        print("Calculating Each LMP pair from Market Result")
        cnt = 1
        sz = len(os.listdir("/Users/jeffreychan/Documents/school/UCR/PhD/Yu/data/2021/MPMA/"))
        for MPMAFile in glob.glob(f"/Users/jeffreychan/Documents/school/UCR/PhD/Yu/data/2021/MPMA/*{month}_AUCTION*"):
            marketResultsFile   = pd.read_csv(MPMAFile)
            print("progress bar: ", round(cnt / sz,2)*100, "%",end="\r")
            for row in range(len(marketResultsFile)):
                source  = marketResultsFile["Source"].iloc[row] 
                sink    = marketResultsFile["Sink"].iloc[row]
                try:
                    source_peak_sum     = totalLMP_SCR_SINK[source][0]
                    source_offpeak_sum  = totalLMP_SCR_SINK[source][1]
                    source_type         = SCR_SINK_TYPE[source]
                except:
                    source_peak_sum     = np.nan
                    source_offpeak_sum  = np.nan
                    source_type         = " "
                try:
                    sink_peak_sum       = totalLMP_SCR_SINK[sink][0]
                    sink_offpeak_sum    = totalLMP_SCR_SINK[sink][1]
                    sink_type           = SCR_SINK_TYPE[sink]
                except:
                    sink_peak_sum       = np.nan 
                    sink_offpeak_sum    = np.nan 
                    sink_type           = " "
                bigBoysPlayers.append(
                    {
                        "MarketParticipant" : marketResultsFile["MarketParticipant"].iloc[row], 
                        "ClearingPrice"     : marketResultsFile["ClearingPrice"].iloc[row],
                        "LMP_PEAK_SCR"      : source_peak_sum,
                        "LMP_OFFPEAK_SCR"   : source_offpeak_sum,
                        "LMP_PEAK_SINK"		: sink_peak_sum,
                        "LMP_OFFPEAK_SINK"  : sink_offpeak_sum,
                        "MW"                : marketResultsFile["MW"].iloc[row],
                        "HedgeType"			: marketResultsFile["HedgeType"].iloc[row],
                        "Class"				: marketResultsFile["Class"].iloc[row],
                        "source"            : source,
                        "source_type"       : source_type,
                        "sink"              : sink,
                        "sink_type"         : sink_type,
                        "type"              : marketResultsFile["Type"].iloc[row],
                        "season"            : month
                    }
                )
            cnt += 1    
    bigBoysPlayers = pd.DataFrame(bigBoysPlayers)
    return bigBoysPlayers
	
def FTRBD(year: int) -> pd.DataFrame:
    print("Processing the ", year)
    bigBoysPlayers  = [] 
    yr              = str(year)[2:]
    quarter = { 
               "Sum"  : (str(year)+"-06-01", str(year)+"-08-31"),
               "Fall" : (str(year)+"-09-01", str(year)+"-11-30"),
               "Win"  : (str(year)+"-12-01", str(year+1)+"-02-28"), 
               "Spr"  : (str(year+1)+"-03-01", str(year+1)+"-05-31")
            }
    file_name_template =  "dam_data_{}.csv"
    for season in quarter: 
        start_date  = dt.datetime.strptime(quarter[season][0] , "%Y-%m-%d")
        end_date    = dt.datetime.strptime(quarter[season][1], "%Y-%m-%d")
        total_days  = (end_date - start_date).days + 1
        totalLMP_SCR_SINK = {}
        SCR_SINK_TYPE = {}
        print("Calculating the LMP sum for each source and sink - ", season)
        for i in range(total_days):
            print("DAM sum progress: ", np.round((i/total_days)*100, 2), "%", end="\r")
            _date       = start_date + dt.timedelta(days=i)
            _file_name  = file_name_template.format(_date.strftime("%Y%m%d"))
            csv_path    =  "/Users/jeffreychan/Documents/school/UCR/PhD/Yu/data/" + _date.strftime("%Y") + "/dam/" + _date.strftime("%Y") + "/"
            _file_path  = os.path.join(csv_path, _file_name)
            df          = pd.read_csv(_file_path, skiprows = 4)
            df          = df[df.Value == "LMP"].reset_index()
            for dataRow in range(len(df)):
                row = df.iloc[dataRow]
                if row["Node"] in totalLMP_SCR_SINK:
                    totalLMP_SCR_SINK[row["Node"]][0] += row[df.columns[10:-2]].sum()
                    totalLMP_SCR_SINK[row["Node"]][1] += row[df.columns[4:10]].sum()+row[df.columns[-2:]].sum()
                else:
                    SCR_SINK_TYPE[row["Node"]] = row["Type"]
                    totalLMP_SCR_SINK[row["Node"]] = [row[df.columns[10:-2]].sum(), (row[df.columns[4:10]].sum()+row[df.columns[-2:]].sum())]
                    
        print("Calculating Each LMP Pair from Bid File")
        for round in range(1,4):
            BidCSV   = (str(year) if season != "Spr" else str(year+1)) + "R" + str(round) + season + ".csv"
            BidFile  = pd.read_csv("../../data/bid/" + BidCSV)
            print("Round: ", round, end="\r")
            for row in range(len(BidFile)):
                source  = BidFile["Source"].iloc[row] 
                sink    = BidFile["Sink"].iloc[row]
                try:
                    source_peak_sum     = totalLMP_SCR_SINK[source][0]
                    source_offpeak_sum  = totalLMP_SCR_SINK[source][1]
                    source_type         = SCR_SINK_TYPE[source]
                except:
                    source_peak_sum     = np.nan
                    source_offpeak_sum  = np.nan
                    source_type         = " "
                try:
                    sink_peak_sum       = totalLMP_SCR_SINK[sink][0]
                    sink_offpeak_sum    = totalLMP_SCR_SINK[sink][1]
                    sink_type           = SCR_SINK_TYPE[sink]
                except:
                    sink_peak_sum       = np.nan 
                    sink_offpeak_sum    = np.nan 
                    sink_type           = " "
                bigBoysPlayers.append(
                    {
                        "AOI"               : BidFile["Asset Owner ID"].iloc[row], 
                        "LMP_PEAK_SCR"      : source_peak_sum,
                        "LMP_OFFPEAK_SCR"   : source_offpeak_sum,
                        "LMP_PEAK_SINK"		: sink_peak_sum,
                        "LMP_OFFPEAK_SINK"  : sink_offpeak_sum,
                        "MW"                : max([BidFile["MW"+str(x)].iloc[row] for x in range (1,11) if BidFile["MW" + str(x)].iloc[row] is not None]),
                        "HedgeType"			: BidFile["Hedge Type"].iloc[row],
						"Class"				: BidFile["Class"].iloc[row],
                        "source"            : source,
                        "source_type"       : source_type,
                        "sink"              : sink,
                        "sink_type"         : sink_type,
                        "type"              : BidFile["Type"].iloc[row],
                        "season"            : season,
                        "round"             : round
                    }
                )
    bigBoysPlayers = pd.DataFrame(bigBoysPlayers)
    return bigBoysPlayers


def FTRDF(year: int) -> pd.DataFrame:
    print("Processing the ", year)
    bigBoysPlayers  = [] 
    yr              = str(year)[2:]
    quarter = { 
               "Sum"+yr                 : (str(year)+"-06-01", str(year)+"-08-31"),
               "Fal"+yr                 : (str(year)+"-09-01", str(year)+"-11-30"),
               "Win"+yr                 : (str(year)+"-12-01", str(year+1)+"-02-28"), 
               "Spr"+ str(year+1)[-2:]  : (str(year+1)+"-03-01", str(year+1)+"-05-31")
            }
    file_name_template =  "dam_data_{}.csv"
    for season in quarter: 
        start_date  = dt.datetime.strptime(quarter[season][0] , "%Y-%m-%d")
        end_date    = dt.datetime.strptime(quarter[season][1], "%Y-%m-%d")
        total_days  = (end_date - start_date).days + 1
        totalLMP_SCR_SINK = {}
        SCR_SINK_TYPE = {}
        print("Calculating the LMP sum for each source and sink - ", season)
        for i in range(total_days):
            print("DAM sum progress: ", np.round((i/total_days)*100, 2), "%", end="\r")
            _date       = start_date + dt.timedelta(days=i)
            _file_name  = file_name_template.format(_date.strftime("%Y%m%d"))
            csv_path    =  "/Users/jeffreychan/Documents/school/UCR/PhD/Yu/data/" + _date.strftime("%Y") + "/dam/" + _date.strftime("%Y") + "/"
            _file_path  = os.path.join(csv_path, _file_name)
            df          = pd.read_csv(_file_path, skiprows = 4)
            df          = df[df.Value == "LMP"].reset_index()
            for dataRow in range(len(df)):
                row = df.iloc[dataRow]
                if row["Node"] in totalLMP_SCR_SINK:
                    totalLMP_SCR_SINK[row["Node"]][0] += row[df.columns[10:-2]].sum()
                    totalLMP_SCR_SINK[row["Node"]][1] += row[df.columns[4:10]].sum()+row[df.columns[-2:]].sum()
                else:
                    SCR_SINK_TYPE[row["Node"]] = row["Type"]
                    totalLMP_SCR_SINK[row["Node"]] = [row[df.columns[10:-2]].sum(), (row[df.columns[4:10]].sum()+row[df.columns[-2:]].sum())]
                    
        print("Calculating Each LMP pair from Market Result")
        for round in range(1,4):
            marketResultsCSV    = "MarketResults_" + season + "_AUCTION_Annual" + yr + "Auc_Round_" + str(round) + ".csv"
            marketResultsFile   = pd.read_csv("../../data/"+ str(year) + "/" + marketResultsCSV)
            print("Round: ", round, end="\r")
            for row in range(len(marketResultsFile)):
                source  = marketResultsFile["Source"].iloc[row] 
                sink    = marketResultsFile["Sink"].iloc[row]
                try:
                    source_peak_sum     = totalLMP_SCR_SINK[source][0]
                    source_offpeak_sum  = totalLMP_SCR_SINK[source][1]
                    source_type         = SCR_SINK_TYPE[source]
                except:
                    source_peak_sum     = np.nan
                    source_offpeak_sum  = np.nan
                    source_type         = " "
                try:
                    sink_peak_sum       = totalLMP_SCR_SINK[sink][0]
                    sink_offpeak_sum    = totalLMP_SCR_SINK[sink][1]
                    sink_type           = SCR_SINK_TYPE[sink]
                except:
                    sink_peak_sum       = np.nan 
                    sink_offpeak_sum    = np.nan 
                    sink_type           = " "
                bigBoysPlayers.append(
                    {
                        "MarketParticipant" : marketResultsFile["MarketParticipant"].iloc[row], 
                        "ClearingPrice"     : marketResultsFile["ClearingPrice"].iloc[row],
                        "LMP_PEAK_SCR"      : source_peak_sum,
                        "LMP_OFFPEAK_SCR"   : source_offpeak_sum,
                        "LMP_PEAK_SINK"		: sink_peak_sum,
                        "LMP_OFFPEAK_SINK"  : sink_offpeak_sum,
                        "MW"                : marketResultsFile["MW"].iloc[row],
                        "HedgeType"			: marketResultsFile["HedgeType"].iloc[row],
						"Class"				: marketResultsFile["Class"].iloc[row],
                        "source"            : source,
                        "source_type"       : source_type,
                        "sink"              : sink,
                        "sink_type"         : sink_type,
                        "type"              : marketResultsFile["Type"].iloc[row],
                        "season"            : season,
                        "round"             : round
                    }
                )
    bigBoysPlayers = pd.DataFrame(bigBoysPlayers)
    return bigBoysPlayers

def Profit(df_org : pd.DataFrame):
    df					= df_org.copy()
    totalPeakProfit		= 0
    totalOffPeakProfit	= 0
    n					= len(df)
    df					= df.dropna().reset_index()
    dropped				= len(df)
    for row in range(len(df)):
        if df.loc[row].Class == "Peak":
            if df.loc[row].type == "BUY":
                if df.loc[row].HedgeType == "OBL":
                    totalPeakProfit += (((df.loc[row].LMP_PEAK_SINK - df.loc[row].LMP_PEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW)
                else:
                    totalPeakProfit += max((((df.loc[row].LMP_PEAK_SINK - df.loc[row].LMP_PEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW),0)
            else:
                if df.loc[row].HedgeType == "OBL":
                    totalPeakProfit += -1*(((df.loc[row].LMP_PEAK_SINK - df.loc[row].LMP_PEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW)
                else:
                    totalPeakProfit += max(-1*(((df.loc[row].LMP_PEAK_SINK - df.loc[row].LMP_PEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW),0)
        else:
            if df.loc[row].type == "BUY":
                if df.loc[row].HedgeType == "OBL":
                    totalOffPeakProfit += (((df.loc[row].LMP_OFFPEAK_SINK - df.loc[row].LMP_OFFPEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW)
                else:
                    totalOffPeakProfit += max((((df.loc[row].LMP_OFFPEAK_SINK - df.loc[row].LMP_OFFPEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW),0)
            else:
                if df.loc[row].HedgeType == "OBL":
                    totalOffPeakProfit += -1*(((df.loc[row].LMP_OFFPEAK_SINK - df.loc[row].LMP_OFFPEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW)
                else:
                    totalPeakProfit += max(-1*(((df.loc[row].LMP_OFFPEAK_SINK - df.loc[row].LMP_OFFPEAK_SCR) * df.loc[row].MW) - (df.loc[row].ClearingPrice)*df.loc[row].MW),0)
        #print(round(row/len(df)*100,2), end='\r')
    totalPeakProfit		= round(totalPeakProfit,2)
    totalOffPeakProfit	= round(totalOffPeakProfit,2)
    totalProfit			= round(totalPeakProfit + totalOffPeakProfit,2)
    #print(f"Total Profit {totalProfit:,} \nTotal Peak Profit: {totalPeakProfit:,}, Total Off Peak Profit: {totalOffPeakProfit:,}\nNumber of rows dropped (Due to N/A): {n-dropped}")
    return totalProfit, totalPeakProfit, totalOffPeakProfit, n-dropped

def standardizedRevenue(df_org : pd.DataFrame):
    return (df_org.Revenue - np.average(df_org.Revenue)) / np.std(df_org.Revenue)

def MPdf(odf : pd.DataFrame) -> pd.DataFrame:
    df = []
    for key in odf.MarketParticipant.unique():
        keyRev, trim = Profit(odf[odf["MarketParticipant"]==key])
        MPdf.append(
            {
                "MarketParticipant" : key,
                "Revenue"           : keyRev,
                "Buy"               : len(odf[(odf["MarketParticipant"]==key) & (odf["type"]=="BUY")]),
                "Sell"              : len(odf[(odf["MarketParticipant"]==key) & (odf["type"]=="SELL")]),
                "TotalMW"           : odf[(odf["MarketParticipant"]==key)].MW.sum(),            
                "SCR_Hub"           : len(odf[(odf["MarketParticipant"]==key) & (odf["source_type"]=="Hub")]),
                "SCR_Interface"     : len(odf[(odf["MarketParticipant"]==key) & (odf["source_type"]=="Interface")]),
                "SCR_Loadzone"      : len(odf[(odf["MarketParticipant"]==key) & (odf["source_type"]=="Loadzone")]),
                "SCR_Gennode"       : len(odf[(odf["MarketParticipant"]==key) & (odf["source_type"]=="Gennode")]),
                "SCR_UNKNOWN"       : len(odf[(odf["MarketParticipant"]==key) & (odf["source_type"]==" ")]),
                "SINK_Hub"          : len(odf[(odf["MarketParticipant"]==key) & (odf["type"]=="Hub")]),
                "SINK_Interface"    : len(odf[(odf["MarketParticipant"]==key) & (odf["type"]=="Interface")]),
                "SINK_Loadzone"     : len(odf[(odf["MarketParticipant"]==key) & (odf["type"]=="Loadzone")]),
                "SINK_Gennode"      : len(odf[(odf["MarketParticipant"]==key) & (odf["type"]=="Gennode")]),
                "SINK_UNKNOWN"      : len(odf[(odf["MarketParticipant"]==key) & (odf["source_type"]==" ")]),
                "Sum21"             : len(odf[(odf["MarketParticipant"]==key) & (odf["season"]== "Sum21")]),
                "Fal21"             : len(odf[(odf["MarketParticipant"]==key) & (odf["season"]== "Fal21")]),
                "Win21"             : len(odf[(odf["MarketParticipant"]==key) & (odf["season"]== "Win21")]),
                "Spr22"             : len(odf[(odf["MarketParticipant"]==key) & (odf["season"]== "Spr22")]),
                "First_Round"       : len(odf[(odf["MarketParticipant"]==key) & (odf["round"]== 1)]),
                "Second_Round"      : len(odf[(odf["MarketParticipant"]==key) & (odf["round"]== 2)]),
                "Third_Round"       : len(odf[(odf["MarketParticipant"]==key) & (odf["round"]== 3)])
            }
        )
    return pd.DataFrame(df)