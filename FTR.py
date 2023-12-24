import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import multiprocessing

marketResultFiles               = [     
                                        ["../../data/2021/MarketResults_Sum21_AUCTION_Annual21Auc_Round_1.csv",
                                        "../../data/2021/MarketResults_Sum21_AUCTION_Annual21Auc_Round_2.csv",
                                        "../../data/2021/MarketResults_Sum21_AUCTION_Annual21Auc_Round_3.csv",
                                        "../../data/2021/MarketResults_Fal21_AUCTION_Annual21Auc_Round_1.csv",
                                        "../../data/2021/MarketResults_Fal21_AUCTION_Annual21Auc_Round_2.csv",
                                        "../../data/2021/MarketResults_Fal21_AUCTION_Annual21Auc_Round_3.csv",
                                        "../../data/2021/MarketResults_Win21_AUCTION_Annual21Auc_Round_1.csv",
                                        "../../data/2021/MarketResults_Win21_AUCTION_Annual21Auc_Round_2.csv",
                                        "../../data/2021/MarketResults_Win21_AUCTION_Annual21Auc_Round_3.csv",
                                        "../../data/2021/MarketResults_Spr22_AUCTION_Annual21Auc_Round_1.csv",
                                        "../../data/2021/MarketResults_Spr22_AUCTION_Annual21Auc_Round_2.csv",
                                        "../../data/2021/MarketResults_Spr22_AUCTION_Annual21Auc_Round_3.csv"],
                                        
                                        ["../../data/2022/MarketResults_Sum22_AUCTION_Annual22Auc_Round_1.csv",
                                        "../../data/2022/MarketResults_Sum22_AUCTION_Annual22Auc_Round_2.csv",
                                        "../../data/2022/MarketResults_Sum22_AUCTION_Annual22Auc_Round_3.csv",
                                        "../../data/2022/MarketResults_Fal22_AUCTION_Annual22Auc_Round_1.csv",
                                        "../../data/2022/MarketResults_Fal22_AUCTION_Annual22Auc_Round_2.csv",
                                        "../../data/2022/MarketResults_Fal22_AUCTION_Annual22Auc_Round_3.csv",
                                        "../../data/2022/MarketResults_Win22_AUCTION_Annual22Auc_Round_1.csv",
                                        "../../data/2022/MarketResults_Win22_AUCTION_Annual22Auc_Round_2.csv",
                                        "../../data/2022/MarketResults_Win22_AUCTION_Annual22Auc_Round_3.csv",
                                        "../../data/2022/MarketResults_Spr23_AUCTION_Annual22Auc_Round_1.csv",
                                        "../../data/2022/MarketResults_Spr23_AUCTION_Annual22Auc_Round_2.csv",
                                        "../../data/2022/MarketResults_Spr23_AUCTION_Annual22Auc_Round_3.csv"]
                                ]
                        
BidFile                         = [    
                                       ["../../data/bid/2021R1Sum.csv", "../../data/bid/2021R2Sum.csv", "../../data/bid/2021R3Sum.csv",
                                        "../../data/bid/2021R1Fall.csv", "../../data/bid/2021R2Fall.csv", "../../data/bid/2021R3Fall.csv",
                                        "../../data/bid/2021R1Win.csv", "../../data/bid/2021R2Win.csv", "../../data/bid/2021R3Win.csv",
                                        "../../data/bid/2022R1Spr.csv", "../../data/bid/2022R2Spr.csv", "../../data/bid/2022R3Spr.csv"],
                                   
                                       ["../../data/bid/2022R1Sum.csv", "../../data/bid/2022R2Sum.csv", "../../data/bid/2022R3Sum.csv",
                                        "../../data/bid/2022R1Fall.csv", "../../data/bid/2022R2Fall.csv", "../../data/bid/2022R3Fall.csv",
                                        "../../data/bid/2022R1Win.csv", "../../data/bid/2022R2Win.csv", "../../data/bid/2022R3Win.csv",
                                        "../../data/bid/2023R1Spr.csv", "../../data/bid/2023R2Spr.csv", "../../data/bid/2023R3Spr.csv"]
                                ]

voteFile    = ["vote21.csv", "vote22.csv"] 
regularFile = ["completeResult21.csv", "completeResult22.csv"]

combindedMR     = []
combindedBid    = []
for listOfFile in marketResultFiles:
    combindedMR = combindedMR+listOfFile    
for listOfFile in BidFile:
    combindedBid = combindedBid+listOfFile

AOIset = set()
for file in combindedBid:
        df = pd.read_csv(file)
        AOIset = AOIset.union(df["Asset Owner ID"])

def PairAOI(dfMR : pd.DataFrame, DFBID : pd.DataFrame, AOISet : set) -> pd.DataFrame:
    result = [] 
    for i in range(len(dfMR)):
        rowFromMarketResultFile         = dfMR.iloc[i].to_dict()
        numberOfclearedAwards           = len(dfMR[dfMR["MarketParticipant"] == rowFromMarketResultFile["MarketParticipant"]])
        rowFromMarketResultFile["Type"] = rowFromMarketResultFile["Type"].capitalize()
        currentdf = DFBID.loc[(DFBID.Source == rowFromMarketResultFile["Source"])\
                              & (DFBID.Sink == rowFromMarketResultFile["Sink"])\
                              & ((DFBID.Type == rowFromMarketResultFile["Type"]) | (DFBID.Type == "Self-Schedule"))\
                              & (DFBID.Class == rowFromMarketResultFile["Class"])]
        for j in range(len(currentdf)):
            matchingAOI                     = []
            curResult                       = rowFromMarketResultFile.copy()
            curResult["Match"]              = 0
            curResult["Cleared_Awards"]     = numberOfclearedAwards
            aRowFromBidFile = currentdf.iloc[j].to_dict()
            if aRowFromBidFile["Type"] == "Self-Schedule":
                if rowFromMarketResultFile["MW"] == aRowFromBidFile["MW1"]:
                    curResult["ABS_ERROR"]  = 0.0
                    curResult["Match_MW"]   = aRowFromBidFile["MW1"]
                    curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                    curResult["Match"]      = 1
                    result.append(curResult)
                    matchingAOI.append(curResult["AOI"])
                continue
            if aRowFromBidFile["Type"] == "Buy"\
                and rowFromMarketResultFile["ClearingPrice"] > aRowFromBidFile["PRICE2"]:
                curResult["ABS_ERROR"]  = rowFromMarketResultFile["MW"]
                curResult["Match_MW"]   = 0.0
                curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                curResult["Match"]      = 1
                result.append(curResult)
                matchingAOI.append(curResult["AOI"])
                continue
            if aRowFromBidFile["Type"] == "Sell"\
                and rowFromMarketResultFile["ClearingPrice"] < aRowFromBidFile["PRICE2"]:
                curResult["ABS_ERROR"]  = rowFromMarketResultFile["MW"]
                curResult["Match_MW"]   = 0.0
                curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                curResult["Match"]      = 1
                result.append(curResult)
                matchingAOI.append(curResult["AOI"])
                continue
            if rowFromMarketResultFile["ClearingPrice"] == aRowFromBidFile["PRICE2"]:
                curResult["Match_MW"]   = (aRowFromBidFile["MW1"]+aRowFromBidFile["MW2"]) * 0.5
                curResult["ABS_ERROR"]  = abs(rowFromMarketResultFile["MW"] - curResult["Match_MW"])
                curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                curResult["Match"]      = 1
                result.append(curResult)
                matchingAOI.append(curResult["AOI"])
                continue
            aRowFromBidFile["PRICE11"] = np.nan
            for k in range(2,11):
                leftmw      = "MW"+str(k)
                rightmw     = "MW"+str(k+1)
                leftprice   = "PRICE"+str(k)
                rightprice  = "PRICE"+str(k+1)
                if aRowFromBidFile["Type"] == "Buy"\
                    and rowFromMarketResultFile["ClearingPrice"] < aRowFromBidFile[leftprice]\
                    and (rowFromMarketResultFile["ClearingPrice"] > aRowFromBidFile[rightprice] or np.isnan(aRowFromBidFile[rightprice])):
                    curResult["Match_MW"]   = aRowFromBidFile[leftmw]
                    curResult["ABS_ERROR"]  = abs(rowFromMarketResultFile["MW"] - curResult["Match_MW"])
                    curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                    curResult["Match"]      = 1
                    break
                if aRowFromBidFile["Type"] == "Sell"\
                    and rowFromMarketResultFile["ClearingPrice"] > aRowFromBidFile[leftprice]\
                    and (rowFromMarketResultFile["ClearingPrice"] < aRowFromBidFile[rightprice] or np.isnan(aRowFromBidFile[rightprice])):
                    curResult["Match_MW"]   = aRowFromBidFile[leftmw]
                    curResult["ABS_ERROR"]  = abs(rowFromMarketResultFile["MW"] - curResult["Match_MW"])
                    curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                    curResult["Match"]      = 1
                    break
                if rowFromMarketResultFile["ClearingPrice"] == aRowFromBidFile[rightprice]:
                    curResult["Match_MW"]   = (aRowFromBidFile[leftmw]+aRowFromBidFile[rightmw]) * 0.5
                    curResult["ABS_ERROR"]  = abs(rowFromMarketResultFile["MW"] - curResult["Match_MW"])
                    curResult["AOI"]        = aRowFromBidFile["Asset Owner ID"]
                    curResult["Match"]      = 1
                    break
            result.append(curResult)
            matchingAOI.append(curResult["AOI"])
        nonMatchingAOI = AOISet - set(matchingAOI)
        for AOI in nonMatchingAOI:
            curResult                       = rowFromMarketResultFile.copy()
            curResult["Match_MW"]           = 0.0
            curResult["Match"]              = 0
            curResult["AOI"]                = AOI
            curResult["Cleared_Awards"]     = numberOfclearedAwards
            curResult["ABS_ERROR"]          = rowFromMarketResultFile["MW"]
            result.append(curResult)
    return pd.DataFrame(result)
       
def spinTheTreadingUpForPairingAssetOwnerID(marketResultIndex):
    print("running the following pair [",combindedMR[marketResultIndex], "|", combindedBid[marketResultIndex],"]")
    marketResultDataFrameBasedOnArgumentMarketResultIndex           = pd.read_csv(combindedMR[marketResultIndex])
    bidFilebySeasonsAndRoundsBasedOnArugmentMarketResultIndex       = pd.read_csv(combindedBid[marketResultIndex])
    searchResult                                                    = PairAOI(marketResultDataFrameBasedOnArgumentMarketResultIndex, bidFilebySeasonsAndRoundsBasedOnArugmentMarketResultIndex, AOIset)
    outputFile                                                      = combindedBid[marketResultIndex].split("/")
    outputFile                                                      = "PairAOI_" + outputFile[-1] + ".gz"
    searchResult.to_csv(outputFile)
    print("Done [",combindedMR[marketResultIndex], "|", combindedBid[marketResultIndex],"]")

def vote(MPTypes : dict) -> None:
    MPTypes    = pd.DataFrame({"MarketParticipant" : MPTypes.keys(), "MP_types" : MPTypes.values()})
    PairResult = []
    for listOfFile in BidFile:
        tempFileList = [] 
        for file in listOfFile:
            outputFile = file.split("/")
            outputFile = "PairAOIv1_" + outputFile[-1] + ".gz"
            tempFileList.append(outputFile)
        PairResult.append(tempFileList)
        
    for i in range(len(PairResult)):
        print(f"Working on [{PairResult[i]}]")
        listOfFile                   = PairResult[i]
        CombindedPairResult          = pd.read_csv(listOfFile[0],index_col=0, dtype={"MarketParticipant": str})
        CombindedPairResult["File"]  = listOfFile[0]
        for j in tqdm(range(1,12)):
            tdf                                             = pd.read_csv(listOfFile[j],index_col=0, dtype={"MarketParticipant": str})
            tdf["File"]                                     = listOfFile[j]
            CombindedPairResult                             =  pd.concat([CombindedPairResult,tdf],ignore_index=True)
        print("Merging and Grouping")
        CombindedPairResultCountFile                        = CombindedPairResult[["MarketParticipant", "AOI", "Match","File"]].groupby(["MarketParticipant", "AOI", "File"]).sum().reset_index()
        CombindedPairResultCountFile.columns                = ["MarketParticipant", "AOI","File", "Match_Count"]
        CombindedPairResultCountFile                        = CombindedPairResultCountFile.groupby(["MarketParticipant", "File"], group_keys=False).apply(lambda x: x.sort_values("Match_Count", ascending=False)).reset_index()
        CombindedPairResultCleared_AwardsFile               = CombindedPairResult[["MarketParticipant", "AOI", "Cleared_Awards", "File"]].groupby(["MarketParticipant", "AOI", "File"]).max().reset_index()
        CombindedPairResultErrorFile                        = CombindedPairResult[["MarketParticipant", "AOI","File", "ABS_ERROR"]]
        CombindedPairResultErrorFile["SSE"]                 = CombindedPairResultErrorFile["ABS_ERROR"]**2
        CombindedPairResultErrorFile                        = CombindedPairResultErrorFile.drop("ABS_ERROR",axis=1)
        CombindedPairResultErrorFile                        = CombindedPairResultErrorFile.groupby(["MarketParticipant", "AOI", "File"]).sum()
        CombindedPairResultErrorFile                        = CombindedPairResultErrorFile.groupby(["MarketParticipant","File"], group_keys=False).apply(lambda x: x.sort_values("SSE", ascending=True)).reset_index()
        CombindedPairResultWithErrorCountFile               = CombindedPairResultCountFile.merge(CombindedPairResultErrorFile, on=["MarketParticipant","AOI","File"]).drop("index",axis=1)
        CombindedPairResultWithErrorCountFile               = CombindedPairResultWithErrorCountFile.merge(CombindedPairResultCleared_AwardsFile, on=["MarketParticipant","AOI","File"])
        CombindedPairResultWithErrorCountFile["Round"]      = CombindedPairResultWithErrorCountFile["File"].str.slice(start=10,stop=16)
        CombindedPairResultWithErrorCountFile["Season"]     = CombindedPairResultWithErrorCountFile["File"].str.slice(start=16,stop=19)
        CombindedPairResultWithErrorCountFile               = CombindedPairResultWithErrorCountFile.sort_values(by=["MarketParticipant","Season", "Round", "Match_Count","SSE"], ascending=[True,True,True,False,True])
        CombindedPairResultWithErrorCountFile["SSE"]        = round(CombindedPairResultWithErrorCountFile["SSE"],2)
        CombindedPairResultVote                             = CombindedPairResult[["MarketParticipant", "AOI", "Match","File"]].groupby(["MarketParticipant", "AOI", "File"]).sum().reset_index()
        CombindedPairResultVote.columns                     = ["MarketParticipant", "AOI","File", "Match_Count"]
        CombindedPairResultVote                             = CombindedPairResultVote.merge(CombindedPairResultWithErrorCountFile, on=["MarketParticipant","AOI","File", "Match_Count"])
        totalFileForMP                                      = CombindedPairResultVote[["MarketParticipant","File"]].groupby("MarketParticipant").nunique().reset_index()
        totalFileForMP.columns                              = ["MarketParticipant","TotalFiles"]
        CombindedPairResultVote                             = CombindedPairResultVote.loc[CombindedPairResultVote["Match_Count"] > 0]
        CombindedPairResultVote                             = CombindedPairResultVote.groupby(["MarketParticipant", "File"], group_keys=False).apply(lambda x: x.sort_values(["Match_Count","SSE"], ascending=[False, True]).head(1)).reset_index()
        CombindedPaireResultVoteSSE                         = CombindedPairResultVote[["MarketParticipant", "AOI", "SSE"]].groupby(["MarketParticipant", "AOI"]).sum().reset_index()
        CombindedPaireResultVoteSSE["SSE"]                  = round(CombindedPaireResultVoteSSE["SSE"],2)
        CombindedPairResultVote                             = CombindedPairResultVote[["MarketParticipant", "AOI"]].groupby("MarketParticipant").value_counts().reset_index()
        CombindedPairResultVote                             = CombindedPairResultVote.merge(totalFileForMP, on="MarketParticipant", how="outer")
        CombindedPairResultVote.columns                     = ["MarketParticipant", "AOI", "VoteCountbyFile", "TotalFiles"]
        CombindedPairResultVote                             = CombindedPairResultVote.merge(CombindedPaireResultVoteSSE, on=["MarketParticipant","AOI"])
        CombindedPairResultVote                             = CombindedPairResultVote.sort_values(by=["MarketParticipant","VoteCountbyFile","SSE"], ascending=[True,False,True])
        totalMWforMP                                        = CombindedPairResult[["MarketParticipant","File", "FTRID", "MW"]].groupby(["MarketParticipant", "FTRID"]).head(1).reset_index()
        totalMWforMPbyFile                                  = totalMWforMP[["MarketParticipant","File","MW"]].groupby(["MarketParticipant", "File"]).sum().reset_index()
        totalMWforMP                                        = totalMWforMP[["MarketParticipant","MW"]].groupby("MarketParticipant").sum().reset_index()
        totalMWforMP.columns                                = ["MarketParticipant", "Cleared_MW"]
        totalMWforMP["Cleared_MW"]                          = round(totalMWforMP["Cleared_MW"],2)
        totalMWforMPbyFile.columns                          = ["MarketParticipant", "File", "Cleared_MW"]
        totalMWforMPbyFile["Cleared_MW"]                    = round(totalMWforMPbyFile["Cleared_MW"],2)
        CombindedPairResultVote                             = CombindedPairResultVote.merge(totalMWforMP, on="MarketParticipant")
        CombindedPairResultWithErrorCountFile               = CombindedPairResultWithErrorCountFile.merge(totalMWforMPbyFile, on=["MarketParticipant","File"])
        CombindedPairResultVote                             = CombindedPairResultVote.merge(MPTypes, on="MarketParticipant")
        CombindedPairResultWithErrorCountFile               = CombindedPairResultWithErrorCountFile.merge(MPTypes, on="MarketParticipant")
        CombindedPairResultOne2One                          = CombindedPairResultVote.groupby("MarketParticipant").head(1).reset_index()
        CombindedPairResultVoteOne2ManyTop3                 = CombindedPairResultVote.groupby(["MarketParticipant"],group_key=False).apply(lambda x: x.sort_values(["VoteCountbyFile","SSE"], ascending=[False,True]).head(3)).reset_index() 
        CombindedPairResultVoteOne2ManyTop3.to_csv(voteFile[i][0:-4]+"_One2ManyTop3.csv")
        CombindedPairResultVote.to_csv(voteFile[i][0:-4]+"_One2Many.csv")
        CombindedPairResultOne2One.to_csv(voteFile[i][0:-4]+"_One2One.csv")
        CombindedPairResultWithErrorCountFile.to_csv(regularFile[i])
        print("Finished merging and grouping")
    print("Done with Vote function")

def createTypes() -> dict:
    verifiedTypes   = pd.read_excel("final_updated.xlsx")
    extractTypes    = { key : value for (key, value) in zip(verifiedTypes["Entity Code"], verifiedTypes["Category"])}
    wantedList      = ["Municipal Utility", "Investor Owned Utility", "Proprietary Trading"]
    shortHandedList = ["MU", "IOU", "PT"]
    for kv in list(extractTypes.keys()):
        for i in range(len(wantedList)):
            if wantedList[i] in extractTypes[kv]:
                extractTypes[kv] = shortHandedList[i]
                break
    return extractTypes
    
def splitByTypes(MPTypes : dict) -> None:
    try:
        os.mkdir("./vote")
    except:
        pass
    #MPTypes     = createTypes()
    uniqueTypes = set(MPTypes.values())
    for file in voteFile:
        iou_mu = []
        df = pd.read_csv(file)
        for ut in uniqueTypes:
            mplist = []
            for k,v in MPTypes.items():
                if v == ut:
                    mplist.append(k)
            if ut == "IOU" or ut == "MU":
                iou_mu += mplist
            tdf = df.loc[df.MarketParticipant.isin(mplist)].reset_index()
            tdf.to_csv("./vote/"+ut+"_"+file)
        tdf = df.loc[df.MarketParticipant.isin(iou_mu)].reset_index()
        tdf.to_csv("./vote/iou_mu_"+file)
    print("Done with split by types")
    
if __name__ == "__main__":
    # pool = multiprocessing.Pool(processes = 10)
    # pool.map(spinTheTreadingUpForPairingAssetOwnerID,range(len(combindedMR)))
    # pool.close()
    # pool.join()
    # print("Done...")
    mptypes = createTypes()
    vote(mptypes)
    splitByTypes(mptypes)
    
